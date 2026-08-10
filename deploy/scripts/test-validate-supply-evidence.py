#!/usr/bin/env python3
"""Deterministic black-box contract tests for validate-supply-evidence.py."""

from __future__ import annotations

import hashlib
import io
import json
import pathlib
import subprocess
import sys
import tarfile
import tempfile


REPOSITORY = "https://github.com/chuanmingliu/openai-gpt-live"
IMAGE = "ghcr.io/chuanmingliu/openai-gpt-live/gateway"
SOURCE_SHA = "0123456789abcdef0123456789abcdef01234567"
SHA256 = "sha256:"


def encoded(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def digest(payload: bytes) -> str:
    return SHA256 + hashlib.sha256(payload).hexdigest()


def descriptor(payload: bytes, media_type: str) -> dict[str, object]:
    return {"mediaType": media_type, "digest": digest(payload), "size": len(payload)}


def make_statement(predicate_type: str, runtime_digest: str, platform: str) -> dict[str, object]:
    subject = [{"name": "_", "digest": {"sha256": runtime_digest.removeprefix(SHA256)}}]
    if predicate_type == "https://spdx.dev/Document":
        predicate = {
            "SPDXID": "SPDXRef-DOCUMENT",
            "spdxVersion": "SPDX-2.3",
            "dataLicense": "CC0-1.0",
            "name": "gpt-live-gateway",
            "documentNamespace": "https://example.invalid/spdx/gpt-live-gateway",
            "creationInfo": {
                "created": "2026-08-09T00:00:00Z",
                "creators": ["Tool: buildkit-syft-scanner"],
            },
        }
    else:
        predicate = {
            "builder": {"id": "https://github.com/docker/build-push-action"},
            "buildType": "https://mobyproject.org/buildkit@v1",
            "materials": [{"uri": REPOSITORY, "digest": {"sha1": SOURCE_SHA}}],
            "invocation": {
                "configSource": {
                    "uri": REPOSITORY,
                    "digest": {"sha1": SOURCE_SHA},
                    "entryPoint": "Dockerfile",
                },
                "parameters": {"frontend": "dockerfile.v0", "args": {"target": "gateway"}},
                "environment": {"platform": platform},
            },
            "metadata": {
                "completeness": {"parameters": True, "environment": True, "materials": True}
            },
        }
    return {
        "_type": "https://in-toto.io/Statement/v1",
        "subject": subject,
        "predicateType": predicate_type,
        "predicate": predicate,
    }


def fixture(mutation: str | None = None) -> tuple[bytes, bytes, str]:
    blobs: dict[str, bytes] = {}
    runtime: dict[str, dict[str, object]] = {}
    attestations: list[dict[str, object]] = []
    for architecture, marker in (("amd64", b"amd64-runtime"), ("arm64", b"arm64-runtime")):
        platform = f"linux/{architecture}"
        runtime_payload = marker
        runtime_descriptor = descriptor(runtime_payload, "application/vnd.oci.image.manifest.v1+json")
        runtime_descriptor["platform"] = {"os": "linux", "architecture": architecture}
        runtime[platform] = runtime_descriptor

        spdx = make_statement("https://spdx.dev/Document", str(runtime_descriptor["digest"]), platform)
        slsa = make_statement("https://slsa.dev/provenance/v0.2", str(runtime_descriptor["digest"]), platform)
        if mutation == "predicate" and architecture == "amd64":
            spdx["predicateType"] = "https://example.invalid/not-spdx"
        if mutation == "subject" and architecture == "amd64":
            spdx["subject"][0]["digest"]["sha256"] = "f" * 64
        if mutation == "source-repository" and architecture == "amd64":
            slsa["predicate"]["materials"][0]["uri"] = "https://github.com/example/other"
        if mutation == "source-sha" and architecture == "amd64":
            slsa["predicate"]["materials"][0]["digest"]["sha1"] = "f" * 40
        if mutation == "builder" and architecture == "amd64":
            slsa["predicate"]["builder"]["id"] = "https://example.invalid/builder"
        if mutation == "build-type" and architecture == "amd64":
            slsa["predicate"]["buildType"] = "https://example.invalid/build"
        if mutation == "target" and architecture == "amd64":
            slsa["predicate"]["invocation"]["parameters"]["args"]["target"] = "worker"
        if mutation == "platform" and architecture == "amd64":
            slsa["predicate"]["invocation"]["environment"]["platform"] = "linux/arm64"

        statement_payloads = {
            "https://spdx.dev/Document": encoded(spdx),
            "https://slsa.dev/provenance/v0.2": encoded(slsa),
        }
        layers = []
        for predicate_type, payload in statement_payloads.items():
            blobs[digest(payload)] = payload
            layer = descriptor(payload, "application/vnd.in-toto+json")
            layer["annotations"] = {"in-toto.io/predicate-type": predicate_type}
            layers.append(layer)
        if mutation == "duplicate-statement" and architecture == "amd64":
            layers.append(dict(layers[0]))
        attestation_manifest = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "artifactType": "application/vnd.docker.attestation.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.empty.v1+json",
                "digest": SHA256 + "0" * 64,
                "size": 2,
                "data": "e30=",
            },
            "layers": layers,
            "subject": {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": runtime_descriptor["digest"],
                "size": runtime_descriptor["size"],
            },
        }
        manifest_payload = encoded(attestation_manifest)
        blobs[digest(manifest_payload)] = manifest_payload
        attestation_descriptor = descriptor(
            manifest_payload, "application/vnd.oci.image.manifest.v1+json"
        )
        reference_digest = str(runtime_descriptor["digest"])
        if mutation == "descriptor-binding" and architecture == "amd64":
            reference_digest = SHA256 + "e" * 64
        attestation_descriptor["annotations"] = {
            "vnd.docker.reference.type": "attestation-manifest",
            "vnd.docker.reference.digest": reference_digest,
        }
        attestation_descriptor["platform"] = {"os": "unknown", "architecture": "unknown"}
        attestations.append(attestation_descriptor)

    manifests = [runtime["linux/amd64"], runtime["linux/arm64"], *attestations]
    if mutation == "extra-runtime":
        manifests.append(
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": SHA256 + "d" * 64,
                "size": 1,
                "platform": {"os": "darwin", "architecture": "amd64"},
            }
        )
    root_index = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": manifests,
    }
    root_payload = encoded(root_index)
    root_digest = digest(root_payload)
    blobs[root_digest] = root_payload
    layout_index = {
        "schemaVersion": 2,
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.index.v1+json",
                "digest": root_digest,
                "size": len(root_payload),
                "annotations": {"org.opencontainers.image.ref.name": "candidate"},
            }
        ],
    }
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w") as bundle:
        files = {
            "oci-layout": encoded({"imageLayoutVersion": "1.0.0"}),
            "index.json": encoded(layout_index),
        }
        files.update({f"blobs/sha256/{name.removeprefix(SHA256)}": body for name, body in blobs.items()})
        for name, body in sorted(files.items()):
            info = tarfile.TarInfo(name)
            info.size = len(body)
            info.mode = 0o600
            info.mtime = 0
            bundle.addfile(info, io.BytesIO(body))
    registry_payload = root_payload
    if mutation == "registry-index":
        changed = dict(root_index)
        changed["annotations"] = {"tampered": "true"}
        registry_payload = encoded(changed)
    return archive.getvalue(), registry_payload, root_digest


def run_case(root: pathlib.Path, mutation: str | None) -> subprocess.CompletedProcess[str]:
    archive, registry_index, expected_digest = fixture(mutation)
    with tempfile.TemporaryDirectory(prefix="gpt-live-supply-") as temp:
        temp_path = pathlib.Path(temp)
        archive_path = temp_path / "candidate.oci.tar"
        index_path = temp_path / "candidate.index.json"
        output_path = temp_path / "validation.json"
        archive_path.write_bytes(archive)
        index_path.write_bytes(registry_index)
        return subprocess.run(
            [
                sys.executable,
                str(root / "deploy/scripts/validate-supply-evidence.py"),
                "--oci-archive",
                str(archive_path),
                "--registry-index",
                str(index_path),
                "--image",
                IMAGE,
                "--digest",
                expected_digest,
                "--source-repository",
                REPOSITORY,
                "--source-sha",
                SOURCE_SHA,
                "--target",
                "gateway",
                "--output",
                str(output_path),
            ],
            cwd=root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )


def main() -> int:
    root = pathlib.Path(__file__).resolve().parents[2]
    valid = run_case(root, None)
    if valid.returncode != 0:
        print(f"valid supply evidence rejected: {valid.stderr}", file=sys.stderr)
        return 1
    for mutation in (
        "extra-runtime",
        "predicate",
        "subject",
        "source-repository",
        "source-sha",
        "builder",
        "build-type",
        "target",
        "platform",
        "descriptor-binding",
        "duplicate-statement",
        "registry-index",
    ):
        result = run_case(root, mutation)
        if result.returncode == 0:
            print(f"supply validator accepted mutation: {mutation}", file=sys.stderr)
            return 1
    print("supply evidence semantic boundary tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
