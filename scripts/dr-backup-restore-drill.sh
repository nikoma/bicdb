#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <db-path> [work-dir]" >&2
  exit 64
fi

db_path=$1
work_dir=${2:-./bicdb-dr-drill}
backup_key_env=${BICDB_BACKUP_KEY_ENV:-BICDB_BACKUP_KEY}

if [[ -z "${!backup_key_env:-}" ]]; then
  echo "missing backup key environment variable: ${backup_key_env}" >&2
  exit 65
fi

bicdb() {
  if [[ -n "${BICDB_BIN:-}" ]]; then
    "${BICDB_BIN}" "$@"
  else
    cargo run -p bicdb-cli -- "$@"
  fi
}

mkdir -p "${work_dir}"
backup_path="${work_dir}/full.bicbackup"
restore_path="${work_dir}/restore"
verify_report="${work_dir}/backup-verify.json"
drill_report="${work_dir}/backup-drill.json"
readiness_report="${work_dir}/restore-readiness.json"

rm -rf "${restore_path}"

bicdb backup create "${db_path}" "${backup_path}" --key-env "${backup_key_env}"
bicdb backup verify "${backup_path}" --key-env "${backup_key_env}" --json > "${verify_report}"
bicdb backup drill "${backup_path}" \
  --target "${restore_path}" \
  --json-out "${drill_report}" \
  --key-env "${backup_key_env}"
bicdb health readiness "${restore_path}" --json > "${readiness_report}"

echo "backup_verify=${verify_report}"
echo "backup_drill=${drill_report}"
echo "restore_readiness=${readiness_report}"
