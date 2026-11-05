# The hosted MCP edition (#579): one image per daily edition, the DB baked in.
#
# Built by the deploy workflow after the site smoke passes (#580), with the
# CI-built database passed as DB_PATH and the run's pipeline-start stamp as
# BUILT_AT. A deploy is atomic by construction: the new machine must answer
# /health before the old one stops. There is no upload path, so there is no
# write surface. CA certificates are installed because FastMCP 4 trusts the
# system store, not certifi.
FROM python:3.12-slim

ARG DB_PATH=data/storage/mirror_market.db
ARG BUILT_AT

# gosu: the entrypoint starts as root only to take ownership of the volume
# mount, then drops to the unprivileged user.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates gosu \
    && update-ca-certificates \
    && apt-get clean

WORKDIR /app
COPY requirements.txt requirements-mcp.txt ./
RUN pip install --no-cache-dir -r requirements-mcp.txt

# Only what the server imports: the code and the committed public reference
# data. .dockerignore keeps every client-record directory and the private
# assumption tier out of the build context, so they cannot be copied by
# accident.
COPY config.py main.py ./
COPY mirror_mcp ./mirror_mcp
COPY app ./app
COPY analysis ./analysis
COPY pipeline ./pipeline
COPY pricing ./pricing
COPY trust ./trust
COPY latency ./latency
COPY fetchers ./fetchers
COPY data/reference ./data/reference

# The edition. COPY fails the build when DB_PATH is absent — a missing DB is
# a failed build, never an empty server (invariant 1).
COPY ${DB_PATH} /app/db/mirror_market.db

RUN test -n "${BUILT_AT}" || (echo "BUILT_AT build arg is required" && exit 1)
ENV MIRROR_MCP_DB_BUILT_AT=${BUILT_AT}
ENV MIRROR_MCP_DB_PATH=/app/db/mirror_market.db
ENV MIRROR_MCP_LOG_PATH=/data/questions.jsonl
ENV MIRROR_MCP_HOST=0.0.0.0
ENV MIRROR_MCP_PORT=8000

RUN useradd --create-home --uid 10001 mirror \
    && mkdir -p /data \
    && chown -R mirror:mirror /app /data
EXPOSE 8000

# Root here on purpose: Fly mounts the volume root-owned, and the entrypoint
# chowns the log directory before `exec gosu mirror` — the server itself never
# runs as root.
ENTRYPOINT ["/app/mirror_mcp/entrypoint.sh"]
