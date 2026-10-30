#!/bin/sh
# Install agent-reviewer, its systemd user units, and an example config.
set -eu

BIN_DIR="${BIN_DIR:-$HOME/.local/bin}"
UNIT_DIR="${UNIT_DIR:-$HOME/.config/systemd/user}"
CONF_DIR="${CONF_DIR:-$HOME/.config/agentreviewer}"
SRC="$(cd "$(dirname "$0")" && pwd)"

install -Dm755 "$SRC/agentreviewer.py" "$BIN_DIR/agent-reviewer"
install -Dm644 "$SRC/systemd/agentreviewer-watch.service" \
    "$UNIT_DIR/agentreviewer-watch.service"
install -Dm644 "$SRC/systemd/agentreviewer-watch.timer" \
    "$UNIT_DIR/agentreviewer-watch.timer"

mkdir -p "$CONF_DIR"
if [ ! -f "$CONF_DIR/config.toml" ]; then
    install -m600 "$SRC/config.example.toml" "$CONF_DIR/config.toml"
    echo "created $CONF_DIR/config.toml — edit it before enabling the timer"
fi

systemctl --user daemon-reload
cat <<EOF
Installed:
  $BIN_DIR/agent-reviewer
  $UNIT_DIR/agentreviewer-watch.{service,timer}
  $CONF_DIR/config.toml

Next steps:
  1. edit $CONF_DIR/config.toml (repo, repo_dir, worktree_root, trigger, agent)
  2. agent-reviewer --seed          # mark existing trigger comments processed
  3. systemctl --user enable --now agentreviewer-watch.timer
EOF
