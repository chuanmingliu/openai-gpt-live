#!/usr/bin/env python3
"""Validate raw BuildKit SPDX/SLSA attestations in an OCI layout export.

The registry index and the OCI-exported index must be byte-identical. Every
attestation manifest and in-toto statement is then read through, and hashed
against, the descriptor graph rooted at that index.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import re
import sys
import tarfile
from typing import Any


OCI_INDEX = "application/vnd.oci.image.index.v1+json"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
IN_TOTO = "application/vnd.in-toto+json"
ATTESTATION_TYPE = "application/vnd.docker.attestation.manifest.v1+json"
SPDX = "https://spdx.dev/Document"
SLSA = "https://slsa.dev/provenance/v0.2"
STATEMENT_TYPES = {"https://in-toto.io/Statement/v0.1", "https://in-toto.io/Statement/v1"}
BUILD_TYPE = "https://mobyproject.org/buildkit@v1"
BUILDER_ID = "https://github.com/docker/build-push-action"
PLATFORMS = ("linux/amd64", "linux/arm64")
SHA256_RE = re.compile(r"^sha256:([0-9a-f]{64})$")
SOURCE_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
IMAGE_RE = re.compile(r"^ghcr\.io/chuanmingliu/openai-gpt-live/(gateway|worker|migrate)$")


class InvalidEvidence(ValueError):
    pass


def fail(message: str) -> None:
    raise InvalidEvidence(message)


def strict_json(payload: bytes, label: str) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                fail(f"{label}: duplicate JSON key {key!r}")
            result[key] = value
        return result

    try:
        text = payload.decode("utf-8")
        decoder = json.JSONDecoder(object_pairs_hook=pairs)
        value, end = decoder.raw_decode(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        fail(f"{label}: invalid JSON: {error}")
    if text[end:].strip():
        fail(f"{label}: trailing JSON content")
    return value


def sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def require_digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        fail(f"{label}: expected lowercase sha256 digest")
    return value


def descriptor_size(descriptor: dict[str, Any], payload: bytes, label: str) -> None:
    size = descriptor.get("size")
    if not isinstance(size, int) or isinstance(size, bool) or size != len(payload):
        fail(f"{label}: descriptor size mismatch")


def normalized_repository(uri: Any) -> str:
    if not isinstance(uri, str):
        return ""
    return uri[:-4] if uri.endswith(".git") else uri


class OCILayout:
    def __init__(self, archive_path: pathlib.Path) -> None:
        if archive_path.stat().st_size > 1_073_741_824:
            fail("OCI archive exceeds 1 GiB")
        try:
            self.archive = tarfile.open(archive_path, "r:*")
        except (tarfile.TarError, OSError) as error:
            fail(f"OCI archive cannot be opened: {error}")
        self.entries: dict[str, tarfile.TarInfo] = {}
        members = self.archive.getmembers()
        if len(members) > 8192:
            fail("OCI archive has too many entries")
        for member in members:
            path = pathlib.PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts or member.name in self.entries:
                fail(f"OCI archive has unsafe or duplicate path {member.name!r}")
            if not (member.isfile() or member.isdir()):
                fail(f"OCI archive has non-regular entry {member.name!r}")
            self.entries[member.name] = member

    def close(self) -> None:
        self.archive.close()

    def read(self, name: str, limit: int) -> bytes:
        member = self.entries.get(name)
        if member is None or not member.isfile():
            fail(f"OCI archive is missing regular file {name!r}")
        if member.size < 0 or member.size > limit:
            fail(f"OCI entry {name!r} exceeds its size limit")
        source = self.archive.extractfile(member)
        if source is None:
            fail(f"OCI entry {name!r} cannot be read")
        payload = source.read(limit + 1)
        if len(payload) != member.size:
            fail(f"OCI entry {name!r} changed size while reading")
        return payload

    def blob(self, digest: str, limit: int) -> bytes:
        matched = SHA256_RE.fullmatch(digest)
        if matched is None:
            fail("invalid OCI blob digest")
        payload = self.read(f"blobs/sha256/{matched.group(1)}", limit)
        if sha256(payload) != digest:
            fail(f"OCI blob {digest} has the wrong content hash")
        return payload


def validate_subject(statement: dict[str, Any], runtime_digest: str, label: str) -> None:
    subjects = statement.get("subject")
    if not isinstance(subjects, list) or len(subjects) != 1:
        fail(f"{label}: expected exactly one platform subject")
    subject = subjects[0]
    if not isinstance(subject, dict) or set(subject) != {"name", "digest"} or subject.get("name") != "_":
        fail(f"{label}: subject must be the exact BuildKit platform subject")
    digest_set = subject.get("digest")
    expected = runtime_digest.removeprefix("sha256:")
    if not isinstance(digest_set, dict) or digest_set != {"sha256": expected}:
        fail(f"{label}: subject digest differs from its runtime manifest")


def validate_spdx(predicate: Any, label: str) -> None:
    if not isinstance(predicate, dict):
        fail(f"{label}: SPDX predicate must be an object")
    if predicate.get("SPDXID") != "SPDXRef-DOCUMENT":
        fail(f"{label}: invalid SPDX document ID")
    if predicate.get("spdxVersion") not in ("SPDX-2.2", "SPDX-2.3"):
        fail(f"{label}: unsupported SPDX schema")
    if predicate.get("dataLicense") != "CC0-1.0":
        fail(f"{label}: invalid SPDX data license")
    for field in ("name", "documentNamespace"):
        if not isinstance(predicate.get(field), str) or not predicate[field]:
            fail(f"{label}: missing SPDX {field}")
    creation = predicate.get("creationInfo")
    if not isinstance(creation, dict) or not isinstance(creation.get("created"), str):
        fail(f"{label}: invalid SPDX creationInfo")
    creators = creation.get("creators")
    if not isinstance(creators, list) or not creators or not all(isinstance(item, str) and item for item in creators):
        fail(f"{label}: invalid SPDX creators")


def exact_source_descriptor(value: Any, repository: str, source_sha: str) -> bool:
    return (
        isinstance(value, dict)
        and normalized_repository(value.get("uri")) == repository
        and value.get("digest") == {"sha1": source_sha}
    )


def validate_slsa(predicate: Any, repository: str, source_sha: str, target: str, platform: str, label: str) -> None:
    if not isinstance(predicate, dict):
        fail(f"{label}: SLSA predicate must be an object")
    if predicate.get("builder") != {"id": BUILDER_ID}:
        fail(f"{label}: provenance builder identity is not the approved Buildx action")
    if predicate.get("buildType") != BUILD_TYPE:
        fail(f"{label}: provenance buildType is not BuildKit v1")
    materials = predicate.get("materials")
    if not isinstance(materials, list) or sum(exact_source_descriptor(item, repository, source_sha) for item in materials) != 1:
        fail(f"{label}: provenance does not contain exactly one expected source material")
    invocation = predicate.get("invocation")
    if not isinstance(invocation, dict):
        fail(f"{label}: missing provenance invocation")
    config_source = invocation.get("configSource")
    if not exact_source_descriptor(config_source, repository, source_sha) or config_source.get("entryPoint") != "Dockerfile":
        fail(f"{label}: provenance configSource differs from the expected repository and commit")
    parameters = invocation.get("parameters")
    if not isinstance(parameters, dict) or parameters.get("frontend") != "dockerfile.v0":
        fail(f"{label}: provenance frontend is not dockerfile.v0")
    args = parameters.get("args")
    if not isinstance(args, dict) or args.get("target") != target:
        fail(f"{label}: provenance target differs from the requested Dockerfile target")
    environment = invocation.get("environment")
    if not isinstance(environment, dict) or environment.get("platform") != platform:
        fail(f"{label}: provenance platform differs from its runtime manifest")
    metadata = predicate.get("metadata")
    completeness = metadata.get("completeness") if isinstance(metadata, dict) else None
    if completeness != {"parameters": True, "environment": True, "materials": True}:
        fail(f"{label}: maximum provenance completeness is required")


def validate_statement(
    payload: bytes,
    predicate_type: str,
    runtime_digest: str,
    repository: str,
    source_sha: str,
    target: str,
    platform: str,
) -> dict[str, str]:
    label = f"{platform} {predicate_type} statement"
    statement = strict_json(payload, label)
    if not isinstance(statement, dict) or set(statement) != {"_type", "subject", "predicateType", "predicate"}:
        fail(f"{label}: invalid in-toto statement shape")
    statement_type = statement.get("_type")
    if statement_type not in STATEMENT_TYPES:
        fail(f"{label}: unsupported in-toto statement type")
    if statement.get("predicateType") != predicate_type:
        fail(f"{label}: predicateType differs from its layer descriptor")
    validate_subject(statement, runtime_digest, label)
    if predicate_type == SPDX:
        validate_spdx(statement.get("predicate"), label)
    elif predicate_type == SLSA:
        validate_slsa(statement.get("predicate"), repository, source_sha, target, platform, label)
    else:
        fail(f"{label}: unexpected predicate type")
    return {"statement_sha256": sha256(payload), "statement_type": str(statement_type)}


def validate(args: argparse.Namespace) -> dict[str, Any]:
    image_match = IMAGE_RE.fullmatch(args.image)
    if image_match is None or image_match.group(1) != args.target:
        fail("image repository and Dockerfile target must be an exact approved pair")
    expected_digest = require_digest(args.digest, "candidate index")
    if not SOURCE_SHA_RE.fullmatch(args.source_sha):
        fail("source SHA must be an exact lowercase Git SHA-1")
    repository = args.source_repository.removesuffix(".git")
    if repository != "https://github.com/chuanmingliu/openai-gpt-live":
        fail("source repository is not the approved repository")

    registry_payload = pathlib.Path(args.registry_index).read_bytes()
    if len(registry_payload) > 4 * 1024 * 1024 or sha256(registry_payload) != expected_digest:
        fail("registry index bytes do not match the candidate digest")
    registry_index = strict_json(registry_payload, "registry index")

    layout = OCILayout(pathlib.Path(args.oci_archive))
    try:
        layout_doc = strict_json(layout.read("oci-layout", 4096), "OCI layout marker")
        if layout_doc != {"imageLayoutVersion": "1.0.0"}:
            fail("invalid OCI layout marker")
        layout_index = strict_json(layout.read("index.json", 1024 * 1024), "OCI layout index")
        if not isinstance(layout_index, dict) or layout_index.get("schemaVersion") != 2:
            fail("invalid OCI layout index")
        roots = layout_index.get("manifests")
        if not isinstance(roots, list) or len(roots) != 1 or not isinstance(roots[0], dict):
            fail("OCI layout must contain exactly one candidate index")
        root = roots[0]
        if root.get("mediaType") != OCI_INDEX or require_digest(root.get("digest"), "OCI layout root") != expected_digest:
            fail("OCI layout root does not identify the candidate index")
        root_payload = layout.blob(expected_digest, 4 * 1024 * 1024)
        descriptor_size(root, root_payload, "OCI layout root")
        if root_payload != registry_payload:
            fail("OCI-exported index differs byte-for-byte from the registry index")
        root_index = strict_json(root_payload, "candidate index")
        if root_index != registry_index:
            fail("registry and OCI index documents differ")
        if not isinstance(root_index, dict) or root_index.get("schemaVersion") != 2 or root_index.get("mediaType") != OCI_INDEX:
            fail("candidate root is not an OCI image index")
        manifests = root_index.get("manifests")
        if not isinstance(manifests, list) or len(manifests) != 4 or not all(isinstance(item, dict) for item in manifests):
            fail("candidate index must contain exactly two runtime and two attestation manifests")

        runtime: dict[str, dict[str, Any]] = {}
        attestation_by_runtime: dict[str, dict[str, Any]] = {}
        for descriptor in manifests:
            if descriptor.get("mediaType") != OCI_MANIFEST:
                fail("candidate index contains a non-OCI manifest")
            require_digest(descriptor.get("digest"), "candidate descriptor")
            size = descriptor.get("size")
            if not isinstance(size, int) or isinstance(size, bool) or size <= 0:
                fail("candidate descriptor has an invalid size")
            platform_doc = descriptor.get("platform")
            if not isinstance(platform_doc, dict):
                fail("candidate descriptor has no platform")
            platform = f"{platform_doc.get('os')}/{platform_doc.get('architecture')}"
            annotations = descriptor.get("annotations")
            is_attestation = isinstance(annotations, dict) and annotations.get("vnd.docker.reference.type") == "attestation-manifest"
            if is_attestation:
                if platform != "unknown/unknown":
                    fail("attestation manifest must use unknown/unknown platform")
                reference = require_digest(annotations.get("vnd.docker.reference.digest"), "attestation reference")
                if reference in attestation_by_runtime:
                    fail("runtime manifest has multiple attestation manifests")
                attestation_by_runtime[reference] = descriptor
            else:
                if platform not in PLATFORMS or set(platform_doc) != {"os", "architecture"}:
                    fail(f"unexpected runtime platform {platform}")
                if platform in runtime:
                    fail(f"duplicate runtime platform {platform}")
                runtime[platform] = descriptor
        if tuple(sorted(runtime)) != tuple(sorted(PLATFORMS)):
            fail("runtime platforms must be exactly linux/amd64 and linux/arm64")
        runtime_digests = {str(item["digest"]) for item in runtime.values()}
        if len(runtime_digests) != 2 or set(attestation_by_runtime) != runtime_digests:
            fail("attestation descriptor references do not exactly match runtime manifests")

        platform_results = []
        for platform in PLATFORMS:
            runtime_descriptor = runtime[platform]
            runtime_digest = str(runtime_descriptor["digest"])
            attestation_descriptor = attestation_by_runtime[runtime_digest]
            attestation_digest = str(attestation_descriptor["digest"])
            manifest_payload = layout.blob(attestation_digest, 1024 * 1024)
            descriptor_size(attestation_descriptor, manifest_payload, f"{platform} attestation manifest")
            manifest = strict_json(manifest_payload, f"{platform} attestation manifest")
            if not isinstance(manifest, dict) or manifest.get("schemaVersion") != 2 or manifest.get("mediaType") != OCI_MANIFEST:
                fail(f"{platform}: invalid attestation manifest")
            artifact_type = manifest.get("artifactType")
            subject = manifest.get("subject")
            if artifact_type is not None or subject is not None:
                if artifact_type != ATTESTATION_TYPE or not isinstance(subject, dict):
                    fail(f"{platform}: invalid OCI artifact attestation identity")
                if subject.get("digest") != runtime_digest or subject.get("mediaType") != OCI_MANIFEST or subject.get("size") != runtime_descriptor.get("size"):
                    fail(f"{platform}: attestation manifest subject differs from runtime descriptor")
            layers = manifest.get("layers")
            if not isinstance(layers, list) or len(layers) != 2 or not all(isinstance(item, dict) for item in layers):
                fail(f"{platform}: exactly SPDX and SLSA statement layers are required")
            predicates: dict[str, dict[str, str]] = {}
            for layer in layers:
                if layer.get("mediaType") != IN_TOTO:
                    fail(f"{platform}: attestation layer is not raw in-toto JSON")
                layer_digest = require_digest(layer.get("digest"), f"{platform} statement layer")
                annotations = layer.get("annotations")
                predicate_type = annotations.get("in-toto.io/predicate-type") if isinstance(annotations, dict) else None
                if predicate_type not in (SPDX, SLSA) or predicate_type in predicates:
                    fail(f"{platform}: statement predicate inventory is not exact")
                statement_payload = layout.blob(layer_digest, 32 * 1024 * 1024)
                descriptor_size(layer, statement_payload, f"{platform} statement layer")
                predicates[predicate_type] = validate_statement(
                    statement_payload,
                    predicate_type,
                    runtime_digest,
                    repository,
                    args.source_sha,
                    args.target,
                    platform,
                )
            if set(predicates) != {SPDX, SLSA}:
                fail(f"{platform}: missing SPDX or SLSA statement")
            platform_results.append(
                {
                    "platform": platform,
                    "runtime_digest": runtime_digest,
                    "attestation_manifest_digest": attestation_digest,
                    "spdx": predicates[SPDX],
                    "provenance": predicates[SLSA],
                }
            )
    finally:
        layout.close()
    return {
        "schema_version": "buildx-supply-validation-v1",
        "status": "passed",
        "subject": f"{args.image}@{expected_digest}",
        "source_repository": repository,
        "source_sha": args.source_sha,
        "builder_id": BUILDER_ID,
        "build_type": BUILD_TYPE,
        "target": args.target,
        "platforms": platform_results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--oci-archive", required=True)
    parser.add_argument("--registry-index", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--digest", required=True)
    parser.add_argument("--source-repository", required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--target", choices=("gateway", "worker", "migrate"), required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main() -> int:
    try:
        args = parse_args()
        result = validate(args)
        output = pathlib.Path(args.output)
        if output.exists() or output.is_symlink():
            fail("output must not already exist")
        payload = json.dumps(result, separators=(",", ":"), sort_keys=True).encode() + b"\n"
        descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(descriptor, payload)
        finally:
            os.close(descriptor)
        return 0
    except (InvalidEvidence, OSError) as error:
        print(f"supply evidence rejected: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
