#!/bin/bash
# Install the laptop-side backup pull: script to ~/.local/bin, launchd agent to ~/Library/LaunchAgents.
set -euo pipefail
cd "$(dirname "$0")"
LABEL=com.natgas-bot.pull-backup
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
mkdir -p "$HOME/.local/bin" "$HOME/Library/LaunchAgents"
install -m 755 pull_backup.sh "$HOME/.local/bin/natgas-pull-backup"
sed "s|__HOME__|$HOME|g" "$LABEL.plist" > "$PLIST"
plutil -lint "$PLIST"
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"
launchctl print "gui/$(id -u)/$LABEL" | grep -E "state|path =" | head -3
