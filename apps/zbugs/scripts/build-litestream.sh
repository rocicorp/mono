#!/usr/bin/env bash
#
# Builds the three native binaries the RMv2 local soak needs into
# apps/zbugs/.litestream/bin (gitignored):
#
#   litestream-v3  rocicorp/litestream @ zero@v0.0.10        (packages/zero/Dockerfile)
#   litestream-v5  rocicorp/litestream @ v0.5.17-zero.1      (packages/zero/Dockerfile)
#   vfs-query      mono/go, `make build` (cgo, -tags vfs)    (go/Makefile)
#
# The v3 binary is needed even though v5 does all of the backing up: the
# change-streamer's PurgeLocker branch is gated on `litestream.executable` --
# the v3 path -- so omitting it silently skips the purge lock and diverges
# from production.
#
# Idempotent: each artifact carries a stamp file naming the ref it was built
# from, and is rebuilt only when the stamp is missing or stale. Pass --force
# to rebuild regardless.
set -euo pipefail

LITESTREAM_V3_REF="zero@v0.0.10"
LITESTREAM_V5_VERSION="0.5.17-zero.1"
LITESTREAM_REPO="https://github.com/rocicorp/litestream.git"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${APP_DIR}/../.." && pwd)"
OUT_DIR="${APP_DIR}/.litestream"
BIN_DIR="${OUT_DIR}/bin"
SRC_DIR="${OUT_DIR}/src"

FORCE=0
if [[ "${1:-}" == "--force" ]]; then
  FORCE=1
fi

log() { printf '[build-litestream] %s\n' "$*" >&2; }

if ! command -v go >/dev/null 2>&1; then
  log "go is required but was not found on PATH."
  log "Install it with:  brew install go"
  exit 1
fi
log "using $(go version)"

if ! command -v git >/dev/null 2>&1; then
  log "git is required but was not found on PATH."
  exit 1
fi

mkdir -p "${BIN_DIR}" "${SRC_DIR}"

# $1 stamp file, $2 expected contents -> 0 when a rebuild is needed
needs_build() {
  local stamp="$1" want="$2" bin="$3"
  if [[ "${FORCE}" == "1" ]]; then return 0; fi
  if [[ ! -x "${bin}" ]]; then return 0; fi
  if [[ ! -f "${stamp}" ]]; then return 0; fi
  [[ "$(cat "${stamp}")" != "${want}" ]]
}

# $1 ref, $2 checkout dir
clone_litestream() {
  local ref="$1" dir="$2"
  if [[ -d "${dir}/.git" ]]; then
    log "reusing checkout ${dir}"
    return
  fi
  rm -rf "${dir}"
  log "cloning ${LITESTREAM_REPO} @ ${ref}"
  git clone --depth 1 --branch "${ref}" "${LITESTREAM_REPO}" "${dir}"
}

build_litestream_v3() {
  local bin="${BIN_DIR}/litestream-v3"
  local stamp="${BIN_DIR}/litestream-v3.stamp"
  if ! needs_build "${stamp}" "${LITESTREAM_V3_REF}" "${bin}"; then
    log "litestream-v3 up to date (${LITESTREAM_V3_REF})"
    return
  fi
  local dir="${SRC_DIR}/litestream-v3"
  clone_litestream "${LITESTREAM_V3_REF}" "${dir}"
  log "building litestream-v3"
  # The Dockerfile pins GOTOOLCHAIN=local so the vendored `toolchain`
  # directive cannot pull an older stdlib; do the same here. `-extldflags
  # -static` is deliberately omitted: darwin has no static libSystem, and cgo
  # stays on because litestream calls mattn/go-sqlite3's file-control API.
  (
    cd "${dir}"
    GOTOOLCHAIN=local CGO_ENABLED=1 go build \
      -ldflags "-s -w -X 'main.Version=0.3.13+z0.0.10'" \
      -tags osusergo,netgo,sqlite_omit_load_extension \
      -o "${bin}" ./cmd/litestream
  )
  printf '%s' "${LITESTREAM_V3_REF}" >"${stamp}"
  log "built ${bin}"
}

build_litestream_v5() {
  local bin="${BIN_DIR}/litestream-v5"
  local stamp="${BIN_DIR}/litestream-v5.stamp"
  if ! needs_build "${stamp}" "${LITESTREAM_V5_VERSION}" "${bin}"; then
    log "litestream-v5 up to date (${LITESTREAM_V5_VERSION})"
    return
  fi
  local dir="${SRC_DIR}/litestream-v5"
  clone_litestream "v${LITESTREAM_V5_VERSION}" "${dir}"
  log "building litestream-v5"
  (
    cd "${dir}"
    CGO_ENABLED=0 go build \
      -ldflags "-s -w -X 'main.Version=${LITESTREAM_V5_VERSION}'" \
      -tags osusergo,netgo,sqlite_omit_load_extension \
      -o "${bin}" ./cmd/litestream
  )
  printf '%s' "${LITESTREAM_V5_VERSION}" >"${stamp}"
  log "built ${bin}"
}

build_vfs_query() {
  local bin="${BIN_DIR}/vfs-query"
  local stamp="${BIN_DIR}/vfs-query.stamp"
  # vfs-query is built from the working tree rather than a pinned ref, so the
  # stamp is the tree's own revision of go/.
  local want
  want="$(git -C "${REPO_ROOT}" rev-parse HEAD:go 2>/dev/null || echo unknown)"
  if ! needs_build "${stamp}" "${want}" "${bin}"; then
    log "vfs-query up to date (go/ @ ${want})"
    return
  fi
  log "building vfs-query from ${REPO_ROOT}/go"
  # cgo (mattn/go-sqlite3) -- this must be built with the host toolchain, so
  # it cannot be lifted out of the Docker build.
  make -C "${REPO_ROOT}/go" build
  cp "${REPO_ROOT}/go/dist/vfs-query" "${bin}"
  printf '%s' "${want}" >"${stamp}"
  log "built ${bin}"
}

build_litestream_v3
build_litestream_v5
build_vfs_query

log "binaries in ${BIN_DIR}:"
ls -l "${BIN_DIR}" >&2
