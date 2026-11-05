#!/bin/sh
# Container entrypoint (#579). Fly attaches the volume at /data owned by root;
# the server runs as the unprivileged `mirror` user and must be able to append
# the question log there, so ownership is taken here, once, before privileges
# are dropped. Anything else failing at this point is a startup failure.
set -eu
LOG_DIR=$(dirname "${MIRROR_MCP_LOG_PATH:?MIRROR_MCP_LOG_PATH is not set}")
mkdir -p "$LOG_DIR"
chown mirror:mirror "$LOG_DIR"
exec gosu mirror python -m mirror_mcp
