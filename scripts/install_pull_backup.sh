#!/bin/bash
# Install the laptop-side jobs (launchd agents in ~/Library/LaunchAgents, run twice a day):
#   com.natgas-bot.pull-backup   pull + verify the newest DB backup from the box
#   com.natgas-bot.futures-bars  archive free delayed 1m NG futures bars (natgas-bot installed as a uv tool)
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p "$HOME/.local/bin" "$HOME/Library/LaunchAgents" "$HOME/natgas-backups/futures"
install -m 755 pull_backup.sh "$HOME/.local/bin/natgas-pull-backup"
# launchd can't read ~/Desktop, so run natgas-bot from a uv tool install (a copy, not the checkout)
uv tool install --force --reinstall .. >/dev/null
for LABEL in com.natgas-bot.pull-backup com.natgas-bot.futures-bars; do
  PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
  sed "s|__HOME__|$HOME|g" "$LABEL.plist" > "$PLIST"
  plutil -lint "$PLIST"
  launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
  launchctl bootstrap "gui/$(id -u)" "$PLIST"
  launchctl print "gui/$(id -u)/$LABEL" | grep -E "state|path =" | head -3
done
