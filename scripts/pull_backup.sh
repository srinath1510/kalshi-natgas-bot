#!/bin/bash
# Pull the newest natgas-bot DB backup from the box to this machine and verify its checksum.
# Installed as ~/.local/bin/natgas-pull-backup and run daily by launchd agent com.natgas-bot.pull-backup.
set -euo pipefail
# Host and remote directory are machine-specific; set them in ~/.config/natgas-bot/backup.env:
#   NATGAS_BACKUP_HOST=<ssh host or alias>
#   NATGAS_BACKUP_REMOTE_DIR=<directory holding natgas-*.db.gz on that host>
CONF="${NATGAS_BACKUP_CONF:-$HOME/.config/natgas-bot/backup.env}"
# shellcheck source=/dev/null
[ -f "$CONF" ] && . "$CONF"
HOST="${NATGAS_BACKUP_HOST:?set NATGAS_BACKUP_HOST in $CONF}"
REMOTE_DIR="${NATGAS_BACKUP_REMOTE_DIR:?set NATGAS_BACKUP_REMOTE_DIR in $CONF}"
DEST="${NATGAS_BACKUP_DIR:-$HOME/natgas-backups}"
KEEP="${NATGAS_BACKUP_KEEP:-7}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=20)

mkdir -p "$DEST"
latest=$(ssh "${SSH_OPTS[@]}" "$HOST" "ls -1 '$REMOTE_DIR'/natgas-*.db.gz | tail -1")
name=$(basename "$latest")
if [ -f "$DEST/$name" ]; then
  echo "$(date -u +%FT%TZ) up to date: $name"
  exit 0
fi
scp -q "${SSH_OPTS[@]}" "$HOST:$latest" "$DEST/$name.part"
remote_sum=$(ssh "${SSH_OPTS[@]}" "$HOST" "sha256sum '$latest'" | cut -d' ' -f1)
local_sum=$(shasum -a 256 "$DEST/$name.part" | cut -d' ' -f1)
if [ "$remote_sum" != "$local_sum" ]; then
  rm -f "$DEST/$name.part"
  echo "$(date -u +%FT%TZ) checksum mismatch for $name" >&2
  exit 1
fi
mv "$DEST/$name.part" "$DEST/$name"
ls -1 "$DEST"/natgas-*.db.gz | sort -r | tail -n +$((KEEP + 1)) | while read -r old; do rm -f "$old"; done
echo "$(date -u +%FT%TZ) pulled $name"
