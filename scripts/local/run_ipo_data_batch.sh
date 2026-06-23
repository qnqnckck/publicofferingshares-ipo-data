#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE="${IPO_DATA_BATCH_ENV:-"$ROOT_DIR/scripts/local/ipo_data_batch.env"}"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

require_var() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    printf 'Missing required env var: %s\n' "$name" >&2
    exit 1
  fi
}

ensure_clean_tracked_tree() {
  if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
    git status --short
    printf 'Tracked working tree changes exist. Commit or stash them before running data batch.\n' >&2
    exit 1
  fi
}

ensure_manual_fundamentals_file() {
  if [[ ! -f data/manual_fundamentals.json ]]; then
    printf '{\n  "stocks": []\n}\n' > data/manual_fundamentals.json
  fi
}

rebuild_derived_data() {
  "$DART_BIN" run tool/ipo_competition_batch.dart \
    --backfill-years 3 \
    --manual-fundamentals-path data/manual_fundamentals.json \
    --no-discover \
    --no-identifier-discover \
    --no-ipo-korea-supplement \
    --no-article-lead-manager-discover \
    --no-public-live-collect
}

public_refresh_data() {
  "$DART_BIN" run tool/ipo_competition_batch.dart \
    --backfill-years 3 \
    --manual-fundamentals-path data/manual_fundamentals.json
}

public_refresh_no_finuts_data() {
  "$DART_BIN" run tool/ipo_competition_batch.dart \
    --backfill-years 3 \
    --manual-fundamentals-path data/manual_fundamentals.json \
    --no-finuts-discover
}

public_live_data() {
  "$DART_BIN" run tool/ipo_competition_batch.dart \
    --backfill-years 3 \
    --manual-fundamentals-path data/manual_fundamentals.json \
    --no-discover \
    --no-identifier-discover \
    --no-ipo-korea-supplement \
    --no-article-lead-manager-discover
}

validate_data() {
  local required="${1:-0}"
  if [[ "$required" == "1" ]]; then
    "$PYTHON_BIN" tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues
  else
    "$PYTHON_BIN" tool/validate_finuts_schedule_sync.py --warn-on-analysis-issues || true
  fi
}

commit_if_changed() {
  local message="$1"
  shift
  local paths=("$@")

  if git diff --quiet -- "${paths[@]}"; then
    log "No data changes."
    return 0
  fi

  if [[ "${IPO_DATA_GIT_COMMIT:-1}" != "1" ]]; then
    log "Skipping git commit because IPO_DATA_GIT_COMMIT=${IPO_DATA_GIT_COMMIT:-}"
    git diff --stat -- "${paths[@]}"
    return 0
  fi

  git config user.name "${IPO_DATA_GIT_USER_NAME:-ipo-competition-local-bot}"
  git config user.email "${IPO_DATA_GIT_USER_EMAIL:-ipo-competition-local-bot@users.noreply.github.com}"
  git add "${paths[@]}"
  git commit -m "$message"

  if [[ "${IPO_DATA_GIT_PUSH:-1}" == "1" ]]; then
    git push
  else
    log "Skipping git push because IPO_DATA_GIT_PUSH=${IPO_DATA_GIT_PUSH:-}"
  fi
}

usage() {
  cat <<'EOF'
Usage:
  scripts/local/run_ipo_data_batch.sh baseline
  scripts/local/run_ipo_data_batch.sh demand
  scripts/local/run_ipo_data_batch.sh live
  scripts/local/run_ipo_data_batch.sh naver-live
  scripts/local/run_ipo_data_batch.sh public-refresh
  scripts/local/run_ipo_data_batch.sh public-refresh-no-finuts
  scripts/local/run_ipo_data_batch.sh rebuild
  scripts/local/run_ipo_data_batch.sh targeted --stock-id <id> [--company <name>] [--mode fundamentals|broker|full]

Batch mapping:
  baseline  공개소스 신규 종목/기본 일정 발굴 + 파생 JSON 재생성
  demand    공개소스 수요예측/fundamentals 갱신 + 파생 JSON 재생성
  live      공개소스 청약일 균등/비례 경쟁률 갱신
  naver-live live와 동일. 네이버 우선, 이후 공개 증권사/커뮤니티 소스 조회
  public-refresh Finuts/DART/iTick/IPOKorea/기사/네이버 등 공개 소스 전체 갱신
  public-refresh-no-finuts DART/iTick/IPOKorea/기사/네이버 등 공개 소스 갱신
  rebuild   repo 데이터만으로 ipo_competition_data 재생성
  targeted  특정 종목 수동 backfill
EOF
}

cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
DART_BIN="${DART_BIN:-dart}"
BATCH="${1:-}"
shift || true

if [[ -z "$BATCH" || "$BATCH" == "-h" || "$BATCH" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${IPO_DATA_GIT_PULL:-1}" == "1" ]]; then
  log "Pulling latest main"
  git pull --rebase origin main
fi

ensure_clean_tracked_tree
ensure_manual_fundamentals_file

case "$BATCH" in
  baseline)
    log "Running public baseline discovery sync"
    public_refresh_data
    validate_data 0
    commit_if_changed "Sync public IPO baseline data" \
      data/discovered data/identifiers ipo_competition_data
    ;;

  demand)
    log "Running public demand and fundamentals refresh"
    public_refresh_data
    validate_data 0
    commit_if_changed "Sync public demand and fundamentals data" \
      data/discovered data/identifiers ipo_competition_data
    ;;

	  live)
	    log "Running public live subscription competition sync"
    public_live_data
    validate_data 1
	    commit_if_changed "Sync public live subscription competition data" \
	      ipo_competition_data
	    ;;

	  naver-live)
	    log "Running public live subscription competition sync"
	    public_live_data
	    git restore -- data/discovered/ipo_events.json data/identifiers/ipo_identifiers.json
	    commit_if_changed "Sync public live subscription competition data" \
	      ipo_competition_data
	    ;;

  public-refresh)
    log "Running full public-source IPO data refresh"
    public_refresh_data
    commit_if_changed "Sync public-source IPO data" \
      data/discovered data/identifiers ipo_competition_data
    ;;

  public-refresh-no-finuts)
    log "Running public-source IPO data refresh without Finuts"
    public_refresh_no_finuts_data
    commit_if_changed "Sync public-source IPO data without Finuts" \
      data/discovered data/identifiers ipo_competition_data
    ;;
	
	  rebuild)
	    log "Rebuilding derived IPO data from repo data"
    rebuild_derived_data
    validate_data 0
    commit_if_changed "Rebuild public derived IPO data" ipo_competition_data
    ;;

  targeted)
    MODE="full"
    STOCK_ID=""
    COMPANY=""
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --stock-id)
          STOCK_ID="${2:-}"
          shift 2
          ;;
        --company)
          COMPANY="${2:-}"
          shift 2
          ;;
        --mode)
          MODE="${2:-full}"
          shift 2
          ;;
        *)
          printf 'Unknown targeted argument: %s\n' "$1" >&2
          exit 1
          ;;
      esac
    done
    if [[ -z "$STOCK_ID" && -z "$COMPANY" ]]; then
      printf 'targeted requires --stock-id or --company\n' >&2
      exit 1
    fi
    case "$MODE" in
      fundamentals|broker|full) ;;
      *)
        printf 'Invalid targeted mode: %s\n' "$MODE" >&2
        exit 1
        ;;
    esac
    log "Running public targeted backfill mode=$MODE stock_id=$STOCK_ID company=$COMPANY"
    if [[ "$MODE" == "fundamentals" ]]; then
      public_refresh_data
    else
      public_live_data
    fi
    validate_data 0
    commit_if_changed "Run public manual targeted backfill" \
      data/discovered data/identifiers ipo_competition_data
    ;;

  *)
    usage
    exit 1
    ;;
esac
