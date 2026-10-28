#!/usr/bin/env bash
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export GG_IMAGE="${GG_IMAGE:-x}"   # unused by 'down' but required by the compose file
docker compose -f "$ROOT/cluster/local/docker-compose.yml" down -v
