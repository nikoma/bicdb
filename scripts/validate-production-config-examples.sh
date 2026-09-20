#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
env_file="${repo_root}/docs/examples/bicdb-production.env"
systemd_file="${repo_root}/docs/examples/bicdb.service"
compose_file="${repo_root}/docs/examples/container-compose.yml"

if [[ ! -f "${env_file}" ]]; then
  echo "missing ${env_file}" >&2
  exit 66
fi
if [[ ! -f "${systemd_file}" || ! -f "${compose_file}" ]]; then
  echo "missing production service/container examples" >&2
  exit 66
fi

for example in "${systemd_file}" "${compose_file}"; do
  if grep -q -- "--allow-remote-no-auth" "${example}"; then
    echo "insecure remote no-auth flag found in ${example}" >&2
    exit 67
  fi
  grep -q -- "--require-auth" "${example}" || {
    echo "missing --require-auth in ${example}" >&2
    exit 67
  }
  grep -q -- "scram-sha-256" "${example}" || {
    echo "missing scram-sha-256 in ${example}" >&2
    exit 67
  }
  grep -q -- "--require-tls" "${example}" || {
    echo "missing --require-tls in ${example}" >&2
    exit 67
  }
done

set -a
# shellcheck source=/dev/null
. "${env_file}"
set +a

bicdb() {
  if [[ -n "${BICDB_BIN:-}" ]]; then
    "${BICDB_BIN}" "$@"
  else
    cargo run -p bicdb-cli -- "$@"
  fi
}

tmp_dir=$(mktemp -d)
trap 'rm -rf "${tmp_dir}"' EXIT

BICDB_DB_PATH="${tmp_dir}/database"
BICDB_TLS_CERT="${tmp_dir}/server.pem"
BICDB_TLS_KEY="${tmp_dir}/server-key.pem"
BICDB_PROTECTED_DATA_EVIDENCE="${tmp_dir}/protected-data-release-gate.json"
BICDB_DEPENDENCY_EVIDENCE="${tmp_dir}/dependency-audit.json"
report="${tmp_dir}/production-gate.json"

mkdir -p "${BICDB_DB_PATH}"
printf '%s\n' "certificate placeholder" > "${BICDB_TLS_CERT}"
printf '%s\n' "private key placeholder" > "${BICDB_TLS_KEY}"
chmod 700 "${BICDB_DB_PATH}"
chmod 600 "${BICDB_TLS_KEY}"
printf '%s\n' '{"passed":true}' > "${BICDB_PROTECTED_DATA_EVIDENCE}"
printf '%s\n' '{"passed":true}' > "${BICDB_DEPENDENCY_EVIDENCE}"

export BICDB_DB_KEY="${BICDB_DB_KEY:-0123456789abcdef0123456789abcdef}"
export BICDB_BACKUP_KEY="${BICDB_BACKUP_KEY:-abcdef0123456789abcdef0123456789}"

bicdb security production-gate "${BICDB_DB_PATH}" \
  --profile "${BICDB_PROFILE}" \
  --host "${BICDB_HOST}" \
  --require-auth \
  --auth-method "${BICDB_AUTH_METHOD}" \
  --require-tls \
  --tls-cert "${BICDB_TLS_CERT}" \
  --tls-key "${BICDB_TLS_KEY}" \
  --db-key-env "${BICDB_DB_KEY_ENV}" \
  --backup-key-env "${BICDB_BACKUP_KEY_ENV}" \
  --audit-retention-days 365 \
  --audit-tamper-evidence \
  --protected-data-evidence "${BICDB_PROTECTED_DATA_EVIDENCE}" \
  --dependency-evidence "${BICDB_DEPENDENCY_EVIDENCE}" \
  --json > "${report}"

if ! grep -q '"passed": true' "${report}"; then
  cat "${report}" >&2
  exit 1
fi

echo "production_config_report=${report}"
