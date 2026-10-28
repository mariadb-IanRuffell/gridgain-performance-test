#!/usr/bin/env bash
# usage: scripts/prepare-data.sh <oltp|analytical|ingest|all> [env] [profile-for-sizing]
# RECREATE=1 drops all workload tables first (required when changing dataset size).
# Creates schema (idempotent) and bulk-loads seed data. Sizing comes from config/defaults.properties + optional profile.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WL="${1:?workload}"; ENV_NAME="${2:-local}"; SIZING="${3:-}"
export JVM_ARGS="${JVM_ARGS:--Xms512m -Xmx2g $(tr "\n" " " < "$ROOT/config/jvm-opens.args")}"   # Ignite 2.x client needs --add-opens on JDK 17+
JMETER="$ROOT/tools/apache-jmeter-${JMETER_VERSION:-5.6.3}/bin/jmeter"
mkdir -p "$ROOT/results/_setup"; rm -f "$ROOT"/results/_setup/*
args=(-q "$ROOT/config/jmeter.properties" -q "$ROOT/config/defaults.properties" -q "$ROOT/config/env/$ENV_NAME.properties")
[ -n "$SIZING" ] && args+=(-q "$ROOT/profiles/$SIZING.properties")
run() { "$JMETER" -n -t "$ROOT/plans/$1.jmx" "${args[@]}" -l "$ROOT/results/_setup/$1.jtl" -j "$ROOT/results/_setup/$1.log"; }
[ "${RECREATE:-0}" = "1" ] && run drop
run schema
case "$WL" in
  all) run load-oltp; run load-analytical ;;
  ingest) ;;
  *) run "load-$WL" ;;
esac
# fail loudly: any failed sample, an empty result file, or a JMeter-level error
for j in "$ROOT"/results/_setup/*.jtl; do
  [ "$(wc -l < "$j")" -gt 1 ] || { echo "!! no samples recorded in $j"; exit 1; }
done
if grep -q ',false,' "$ROOT/results/_setup/"*.jtl; then echo "!! setup errors:"; grep -h ',false,' "$ROOT/results/_setup/"*.jtl | head; exit 1; fi
if grep -l 'Uncaught Exception\|ERROR' "$ROOT/results/_setup/"*.log >/dev/null 2>&1; then echo "!! errors in jmeter logs (results/_setup/*.log)"; exit 1; fi
echo ">> data ready"
