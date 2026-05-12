#!/usr/bin/env bash
set -euo pipefail

LABEL="${IPO_DATA_SCHEDULER_LABEL:-com.yellowrocket.publicofferingshares.ipo-data-batches}"
PLIST_PATH="$HOME/Library/LaunchAgents/$LABEL.plist"

launchctl bootout "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || true
rm -f "$PLIST_PATH"
echo "Uninstalled $LABEL"
