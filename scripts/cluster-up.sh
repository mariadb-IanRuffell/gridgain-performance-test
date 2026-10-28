#!/usr/bin/env bash
# Start the local 3-node GridGain 8.10 cluster, activate it (persistence is on) and wait until all nodes are in the topology.
# License: GG_LICENSE_FILE (default ./gridgain-license.xml). With a license -> gridgain/ultimate:$GG_VERSION,
# without -> gridgain/community:$GG_COMMUNITY_TAG (beta).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
[ -f "$ROOT/config/versions.env" ] && set -a && . "$ROOT/config/versions.env" && set +a
[ -f "$ROOT/config/local.env" ] && set -a && . "$ROOT/config/local.env" && set +a
LICENSE_FILE="${GG_LICENSE_FILE:-$ROOT/gridgain-license.xml}"
GEN="$ROOT/cluster/local/.generated"; mkdir -p "$GEN"
REST="http://localhost:10300/ignite"

if [ -f "$LICENSE_FILE" ]; then
  export GG_IMAGE="gridgain/ultimate:$GG_VERSION"; cp "$LICENSE_FILE" "$GEN/license.xml"
  PLUGIN='<property name="pluginConfigurations"><list><bean class="org.gridgain.grid.configuration.GridGainConfiguration"><property name="licenseUrl" value="file:///opt/gridgain/license.xml"/></bean></list></property>'
else
  export GG_IMAGE="gridgain/community:$GG_COMMUNITY_TAG"; : > "$GEN/license.xml"; PLUGIN=''
  echo "!! no license file at $LICENSE_FILE - using the Community edition ($GG_IMAGE)"
fi
python3 - "$ROOT/cluster/local/gridgain-config.xml" "$GEN/perf-config.xml" "$PLUGIN" <<'PY'
import sys; s=open(sys.argv[1]).read().replace("<!--LICENSE_PLUGIN-->", sys.argv[3]); open(sys.argv[2],"w").write(s)
PY
export GG_VERSION
docker compose -f "$ROOT/cluster/local/docker-compose.yml" up -d

rest() { curl -s -m 5 "$REST?cmd=$1" 2>/dev/null; }
echo -n ">> waiting for REST"
until rest version | grep -q '"successStatus":0'; do echo -n .; sleep 2; done; echo
echo -n ">> waiting for 3 nodes in the topology"
until [ "$(rest top | grep -o '"consistentId"' | wc -l | tr -d ' ')" -ge 3 ]; do echo -n .; sleep 2; done; echo
if ! rest currentstate | grep -q '"response":true'; then
  rest activate >/dev/null
  until rest currentstate | grep -q '"response":true'; do sleep 2; done
fi
for port in 10300 10301 10302; do
  until curl -s -m 5 "http://localhost:$port/ignite?cmd=version" | grep -q '"successStatus":0'; do sleep 2; done
done
echo ">> cluster ready (GridGain $GG_VERSION, image $GG_IMAGE, active)"
