#!/bin/sh
set -eu

: "${GITHUB_ENV:?GITHUB_ENV is required}"
: "${INPUT_CONFIG_HASH:?config hash is required}"
: "${INPUT_DEPLOYMENT_REGION:?deployment region is required}"
: "${INPUT_EGRESS_PROXY_CIDR:?egress proxy CIDR is required}"
: "${INPUT_OIDC_ISSUER:?OIDC issuer is required}"
: "${INPUT_PRODUCTION_HOST:?production host is required}"
: "${INPUT_PRODUCTION_TLS_SECRET:?production TLS Secret is required}"
: "${INPUT_RUN_PROVIDER_TESTS:?provider-test decision is required}"
: "${INPUT_TRUSTED_PROXY_CIDR:?trusted proxy CIDR is required}"
: "${INPUT_VOICE_URL:?voice URL is required}"

single_line() {
  name=$1 value=$2
  case "$value" in *"
"*|*""*) echo "$name must be a single bounded value" >&2; exit 64 ;; esac
  case "$value" in *[[:space:]]*) echo "$name must not contain whitespace" >&2; exit 64 ;; esac
  [ "${#value}" -le 2048 ] || { echo "$name is too long" >&2; exit 64; }
}
for name in CONFIG_HASH DEPLOYMENT_REGION EGRESS_PROXY_CIDR OIDC_ISSUER PRODUCTION_HOST PRODUCTION_TLS_SECRET RUN_PROVIDER_TESTS TRUSTED_PROXY_CIDR VOICE_URL; do
  value=$(printenv "INPUT_$name") || { echo "$name is required" >&2; exit 64; }
  single_line "$name" "$value"
done
case "$INPUT_CONFIG_HASH" in v1:????????????????????????????????????????????????????????????????) ;; *) echo 'config hash is malformed' >&2; exit 64 ;; esac
case "$INPUT_DEPLOYMENT_REGION" in *[!a-z0-9-]*|''|-*|*-) echo 'deployment region is malformed' >&2; exit 64 ;; esac
case "$INPUT_PRODUCTION_HOST" in *[!A-Za-z0-9.-]*|''|.*|*.) echo 'production host is malformed' >&2; exit 64 ;; esac
case "$INPUT_PRODUCTION_TLS_SECRET" in *[!a-z0-9.-]*|''|.*|*.) echo 'TLS Secret name is malformed' >&2; exit 64 ;; esac
case "$INPUT_RUN_PROVIDER_TESTS" in true|false) ;; *) echo 'provider-test decision must be true or false' >&2; exit 64 ;; esac
case "$INPUT_OIDC_ISSUER" in https://*) ;; *) echo 'OIDC issuer must be HTTPS' >&2; exit 64 ;; esac
case "$INPUT_VOICE_URL" in wss://*) ;; *) echo 'voice URL must be WSS' >&2; exit 64 ;; esac
case "$INPUT_TRUSTED_PROXY_CIDR" in */*) ;; *) echo 'trusted proxy CIDR is malformed' >&2; exit 64 ;; esac
case "$INPUT_EGRESS_PROXY_CIDR" in */*) ;; *) echo 'egress proxy CIDR is malformed' >&2; exit 64 ;; esac

umask 077
{
  printf 'RELEASE_INPUT_deployment_region=%s\n' "$INPUT_DEPLOYMENT_REGION"
  printf 'RELEASE_INPUT_egress_proxy_cidr=%s\n' "$INPUT_EGRESS_PROXY_CIDR"
  printf 'RELEASE_INPUT_oidc_issuer=%s\n' "$INPUT_OIDC_ISSUER"
  printf 'RELEASE_INPUT_production_host=%s\n' "$INPUT_PRODUCTION_HOST"
  printf 'RELEASE_INPUT_production_tls_secret=%s\n' "$INPUT_PRODUCTION_TLS_SECRET"
  printf 'RELEASE_INPUT_run_provider_tests=%s\n' "$INPUT_RUN_PROVIDER_TESTS"
  printf 'RELEASE_INPUT_trusted_proxy_cidr=%s\n' "$INPUT_TRUSTED_PROXY_CIDR"
  printf 'RELEASE_INPUT_voice_url=%s\n' "$INPUT_VOICE_URL"
} >>"$GITHUB_ENV"
