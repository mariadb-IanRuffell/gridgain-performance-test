#!/usr/bin/env bash
# usage: scripts/run-test.sh <profile> [env]     e.g. scripts/run-test.sh oltp-steady local
# Runs one measured test, then builds the report into results/<run>/report.html
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROFILE_NAME="${1:?profile name (see profiles/)}"; ENV_NAME="${2:-local}"; shift; shift || true   # remaining args: key=value overrides
[ -f "$ROOT/config/versions.env" ] && . "$ROOT/config/versions.env"
JMETER_VERSION="${JMETER_VERSION:-5.6.3}"
JMETER="$ROOT/tools/apache-jmeter-$JMETER_VERSION/bin/jmeter"
PROFILE="$ROOT/profiles/$PROFILE_NAME.properties"; ENVF="$ROOT/config/env/$ENV_NAME.properties"
[ -f "$PROFILE" ] && [ -f "$ENVF" ] || { echo "missing profile or env file"; exit 1; }
prop() { { grep -E "^$1=" "$2" || true; } | tail -1 | cut -d= -f2-; }   # missing key => empty, never an error
WORKLOAD=$(prop workload "$PROFILE"); MODE=$(prop mode "$PROFILE")

if grep -q "^chaos_events=" "$PROFILE"; then
  python3 "$ROOT/framework/chaos.py" --preflight "$ENVF" || exit 1
fi
RUN_ID="$(date +%Y%m%d-%H%M%S)-$PROFILE_NAME-$ENV_NAME"
OUT="$ROOT/results/$RUN_ID"; mkdir -p "$OUT"
cp "$PROFILE" "$OUT/profile.properties"; cp "$ENVF" "$OUT/env.properties"
for kv in "$@"; do echo "$kv" >> "$OUT/profile.properties"; done   # overrides win (last value wins everywhere)
PROFILE="$OUT/profile.properties"; echo "$OUT" > "$ROOT/results/.last_run"
CONN_MODE=$(prop conn_mode "$PROFILE")   # "thread" = one connection per virtual user (needed for very high thread counts)
SALT=$(( $(date +%s) - 1735689600 ))
export JVM_ARGS="${JVM_ARGS:--Xms1g -Xmx${JMETER_HEAP:-4g} -XX:+UseG1GC -Xss512k -XX:-MaxFDLimit $(tr "\n" " " < "$ROOT/config/jvm-opens.args") ${EXTRA_JVM_ARGS:-}}"   # -MaxFDLimit: macOS JVMs otherwise cap fds at 10240
ulimit -n 200000 2>/dev/null || ulimit -n 100000 2>/dev/null || ulimit -n 65536 2>/dev/null || true

echo ">> $RUN_ID  workload=$WORKLOAD mode=$MODE"
"$JMETER" -n -t "$ROOT/plans/run-$WORKLOAD$([ "$CONN_MODE" = thread ] && echo -tc).jmx" \
  -q "$ROOT/config/jmeter.properties" -q "$ROOT/config/defaults.properties" -q "$ENVF" -q "$PROFILE" \
  -Jrun_salt="$SALT" -l "$OUT/results.jtl" -j "$OUT/jmeter.log" &
JPID=$!
# exit fallback: once JMeter says the test ended, give it 30s to exit (leaked non-daemon client threads can keep the JVM alive)
( while kill -0 $JPID 2>/dev/null; do
    if grep -q "Notifying test listeners of end of test" "$OUT/jmeter.log" 2>/dev/null; then sleep 30; kill -9 $JPID 2>/dev/null; break; fi
    sleep 5
  done ) &
# hard stop: if JMeter has not finished well after the planned duration (hung threads), stop it
HARD=$(( $(prop duration "$PROFILE") + $(prop startup_delay "$PROFILE" | grep -E "^[0-9]+$" || echo 0) + 300 ))
( sleep "$HARD"; if kill -0 $JPID 2>/dev/null; then echo "!! hard timeout: stopping JMeter" >&2; python3 -c "import socket;socket.socket(socket.AF_INET,socket.SOCK_DGRAM).sendto(b'StopTestNow',('127.0.0.1',4445))"; sleep 60; kill -9 $JPID 2>/dev/null; fi ) &
START=$(date +%s)

BG=()
if [ "$(prop collector "$ENVF")" = "docker" ]; then
  "$ROOT/framework/collect_docker_stats.sh" "$OUT/resources.csv" "$JPID" "$(prop collector_containers "$ENVF")" & BG+=($!)
fi
if [ "$MODE" = "ramp" ]; then
  python3 "$ROOT/framework/breakpoint_watch.py" "$OUT/results.jtl" "$OUT/profile.properties" "$JPID" "$OUT" & BG+=($!)
fi
if grep -qE "^(chaos_events|probe)=" "$PROFILE" && grep -q "^probe_targets=" "$ENVF"; then
  java @"$ROOT/config/jvm-opens.args" -cp "$ROOT/tools/apache-jmeter-$JMETER_VERSION/lib/*" "$ROOT/framework/Probe.java" "$OUT/probe.csv" "$JPID" "$OUT/env.properties" "$OUT/profile.properties" >"$OUT/probe.log" 2>&1 & BG+=($!)
fi
if grep -q "^chaos_events=" "$PROFILE"; then
  python3 "$ROOT/framework/chaos.py" "$OUT" "$OUT/profile.properties" "$OUT/env.properties" "$JPID" 2>&1 | tee "$OUT/chaos.log" & BG+=($!)
fi
trap 'kill $JPID 2>/dev/null || true' INT TERM
wait $JPID || echo "!! jmeter exited non-zero"
END=$(date +%s)
for b in "${BG[@]:-}"; do [ -n "$b" ] && wait "$b" 2>/dev/null || true; done

GG_VERSION="${GG_VERSION:-unknown}" python3 - "$OUT" "$START" "$END" <<'PY'
import json,sys,platform,os
out,s,e=sys.argv[1],int(sys.argv[2]),int(sys.argv[3])
json.dump({"gridgain_version":os.environ.get("GG_VERSION","unknown"),"start_epoch":s,"end_epoch":e,"host":platform.node(),"loadgen":platform.platform()},open(out+"/run.json","w"))
PY
# node logs: kept only if something went wrong (failed ops, faults, outages, breakpoint); must run before the cluster is torn down
python3 "$ROOT/framework/collect_logs.py" "$OUT" || echo "!! node log collection failed (continuing)"
python3 "$ROOT/framework/report.py" "$OUT"
echo ">> report: $OUT/report.html"
