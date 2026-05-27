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

STATE_DIR="$ROOT_DIR/scripts/local/.state"
LOCK_DIR="$STATE_DIR/scheduler.lock"
mkdir -p "$STATE_DIR"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "Another scheduler tick is still running."
  exit 0
fi
trap 'rmdir "$LOCK_DIR"' EXIT

now_key() {
  TZ=Asia/Seoul date "$1"
}

minute_of_day() {
  local value="$1"
  local hour="${value%%:*}"
  local minute="${value##*:}"
  echo $((10#$hour * 60 + 10#$minute))
}

run_once_per_slot() {
  local batch="$1"
  local slot="$2"
  shift 2
  local marker="$STATE_DIR/${batch}_${slot}.done"
  if [[ -f "$marker" ]]; then
    echo "Skipping $batch for already completed slot $slot"
    return 0
  fi
  "$ROOT_DIR/scripts/local/run_ipo_data_batch.sh" "$batch" "$@"
  date '+%Y-%m-%d %H:%M:%S' > "$marker"
}

weekday="$(now_key '+%u')"
if [[ "$weekday" -gt 5 ]]; then
  echo "Weekend in KST; no scheduled data batch."
  exit 0
fi

date_key="$(now_key '+%Y%m%d')"
time_hm="$(now_key '+%H:%M')"
now_min="$(minute_of_day "$time_hm")"

live_start="$(minute_of_day "${IPO_DATA_LIVE_START:-09:00}")"
live_end="$(minute_of_day "${IPO_DATA_LIVE_END:-17:30}")"

# 청약일 균등/비례 경쟁률. 실행 자체는 10분마다 허용하고,
# 기본값은 피너츠 계정 없이 네이버 IPO 계산기와 공개 증권사/커뮤니티
# 페이지를 순차 조회한다.
if [[ "$now_min" -ge "$live_start" && "$now_min" -le "$live_end" ]]; then
  slot="${date_key}_$(now_key '+%H%M')"
  run_once_per_slot "${IPO_DATA_LIVE_BATCH:-naver-live}" "$slot"
fi

# 수요예측/fundamentals 갱신: GitHub Actions의 18:00, 18:20 KST 대응.
if [[ "${IPO_DATA_DEMAND_ENABLED:-0}" == "1" && ( "$time_hm" == "18:00" || "$time_hm" == "18:20" ) ]]; then
  run_once_per_slot "${IPO_DATA_DEMAND_BATCH:-public-refresh}" "${date_key}_${time_hm/:/}"
fi

# 신규 종목/기본 일정 발굴: GitHub Actions의 17:30 KST 대응.
if [[ "${IPO_DATA_BASELINE_ENABLED:-0}" == "1" && "$time_hm" == "17:30" ]]; then
  run_once_per_slot "${IPO_DATA_BASELINE_BATCH:-public-refresh}" "$date_key"
fi
