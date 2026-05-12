#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LABEL="${IPO_DATA_SCHEDULER_LABEL:-com.yellowrocket.publicofferingshares.ipo-data-batches}"
INTERVAL_SECONDS="${1:-${IPO_DATA_SCHEDULER_INTERVAL_SECONDS:-600}}"
PLIST_PATH="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG_DIR="$ROOT_DIR/build/local_scheduler/logs"

mkdir -p "$HOME/Library/LaunchAgents" "$LOG_DIR"

cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>$ROOT_DIR/scripts/local/ipo_data_scheduler_tick.sh</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$ROOT_DIR</string>
  <key>StartInterval</key>
  <integer>$INTERVAL_SECONDS</integer>
  <key>RunAtLoad</key>
  <false/>
  <key>StandardOutPath</key>
  <string>$LOG_DIR/ipo-data-scheduler.out.log</string>
  <key>StandardErrorPath</key>
  <string>$LOG_DIR/ipo-data-scheduler.err.log</string>
</dict>
</plist>
EOF

chmod +x "$ROOT_DIR/scripts/local/"*.sh
launchctl bootout "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"
launchctl enable "gui/$(id -u)/$LABEL"

echo "Installed $LABEL"
echo "Plist: $PLIST_PATH"
echo "Interval: ${INTERVAL_SECONDS}s"
echo "Logs: $LOG_DIR"
echo "Run now: launchctl kickstart -k gui/$(id -u)/$LABEL"
