#!/usr/bin/env bash
set -euo pipefail

command -v rg >/dev/null || {
  printf 'open-core boundary error: required command rg is missing\n' >&2
  exit 2
}

# Only exit 1 means "no matches". Permission, syntax and I/O errors must
# never turn an unavailable scan into a green boundary check.
scan() {
  local status=0
  rg "$@" || status=$?
  case "$status" in
    0|1) return "$status" ;;
    *) printf 'open-core boundary error: scanner failed (%s)\n' "$status" >&2; exit "$status" ;;
  esac
}

repo_root=$(cd "$(dirname "$0")/.." && pwd -P)
failed=0

fail() {
  printf 'open-core boundary error: %s\n' "$*" >&2
  failed=1
}

while IFS= read -r manifest; do
  manifest_dir=$(dirname "${repo_root}/${manifest}")
  while IFS= read -r relative_path; do
    resolved=$(realpath -m "${manifest_dir}/${relative_path}")
    case "${resolved}" in
      "${repo_root}"|"${repo_root}"/*) ;;
      *) fail "${manifest} has a path dependency outside BicDB: ${relative_path}" ;;
    esac
  done < <(sed -nE 's/.*path[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' "${repo_root}/${manifest}")
done < <(cd "${repo_root}" && git ls-files '*Cargo.toml')

if scan -n -i 'bicdb-platform|bicdb-integrations' "${repo_root}" \
  --glob 'Cargo.toml' --glob 'Cargo.lock'; then
  fail "public dependency manifests mention a private sibling repository"
fi

if scan -n 'AutomaticFleetPlacementPlanner|AutomaticFleetControllerService|AutomaticDistributionHostService' \
  "${repo_root}/crates" --glob '*.rs'; then
  fail "a proprietary automatic fleet implementation is present in public crates"
fi

# Compatibility parsers may still recognize historical serialized names, but
# new externally reachable Rust symbols must be product- and industry-neutral.
if scan -n '^\s*pub (struct|enum|trait|type|const|static|fn)\s+[^\n]*(Carrier|WalkNorth|HL7|Hl7|EHR|Ehr|ERP|Erp|Healthcare|Clinical|PHI|Phi|carrier_|walknorth_|hl7_|ehr_|erp_|healthcare_|clinical_|phi_)' \
  "${repo_root}/crates" --glob '*.rs'; then
  fail "an externally reachable public Rust symbol contains product/vertical vocabulary"
fi

# Textual public contracts are just as binding as Rust symbols. Product names
# in protobuf packages, WIT worlds, HTTP headers, or example manifests would
# make downstream controllers adopt a product-specific BicDB worldview.
if scan -n -i '\b(carrier|walknorth|hl7|ehr|erp|healthcare|clinical|phi)\b|carrier[_:./-]|walknorth' \
  "${repo_root}/abi" "${repo_root}/examples" \
  "${repo_root}/crates/bicdb-provider-grpc/proto" \
  --glob '*.proto' --glob '*.wit' --glob '*.json' --glob '*.toml' \
  --glob '!**/compatibility/**'; then
  fail "a public textual contract or example contains product/vertical vocabulary"
fi

canonical_docs=(
  README.md
  ROADMAP.md
  IMPORTANT-TODO.md
  SERVER_MODE.md
  docs/public-controller-apis.md
  docs/application-runtime-abi-v2.md
  docs/application-runtime-author-guide.md
  docs/application-runtime-operator-guide.md
  docs/application-runtime-implementation-checklist.md
)
doc_matches=$(scan -n -i '\b(carrier|walknorth|hl7|ehr|erp|healthcare|clinical|phi)\b|carrier[_:./-]|walknorth' \
  "${canonical_docs[@]/#/${repo_root}/}") || {
  status=$?
  [[ $status == 1 ]] || exit "$status"
}
while IFS= read -r match; do
  [[ -n $match ]] || continue
  # Legal attribution is not a product-specific public API. Allow only
  # this exact ownership statement in README; preserve the legal notice.
  if [[ $match == "${repo_root}/README.md:"* ]]; then
    doc_record=${match#"${repo_root}/README.md:"}
    doc_line=${doc_record%%:*}
    doc_text=${doc_record#*:}
    if [[ $doc_line =~ ^[0-9]+$ && $doc_text == 'project trademarks. BicDB™ is a trademark jointly owned by WalkNorth, Inc. and' ]]; then
      continue
    fi
  fi
  printf '%s\n' "$match"
  fail "canonical public documentation contains product/vertical vocabulary"
done <<< "$doc_matches"

# Canonical emitters must never reintroduce the former product namespace.
# Legacy spellings are allowed only in explicit decode/fallback paths covered
# by docs/legacy-application-compatibility.md.
if scan -n 'tracer\("bicdb-carrier|meter\("bicdb-carrier|logger\("bicdb-carrier|set_target\("bicdb-carrier|emit_log\("carrier\.|map_attributes\("carrier\.|metadata\.insert\("carrier_|"x-carrier-lifecycle"[[:space:]]*:|"carrier_key_version"[[:space:]]*:|algorithm:[[:space:]]*"carrier-aes' \
  "${repo_root}/crates" --glob '*.rs'; then
  fail "a canonical runtime emitter uses a legacy product namespace"
fi

for extracted_path in \
  docs/rfcs/bicui-database-native-ui-runtime.md \
  docs/browser-platform-roadmap.md \
  docs/future-enterprise-roadmap.md \
  docs/scale-out-storage-backup-todo.md \
  docs/storage-size-reduction.md; do
  if [[ -e "${repo_root}/${extracted_path}" ]]; then
    fail "extracted product/commercial document returned to public BicDB: ${extracted_path}"
  fi
done

if [[ ${failed} -ne 0 ]]; then
  exit 1
fi

cargo metadata --manifest-path "${repo_root}/Cargo.toml" --no-deps --locked \
  --format-version 1 >/dev/null
printf 'open-core dependency direction ok\n'
