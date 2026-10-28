#!/usr/bin/env bash
# Downloads Apache JMeter and the GridGain 9 JDBC driver into ./tools (idempotent).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JMETER_VERSION="${JMETER_VERSION:-5.6.3}"
[ -f "$ROOT/config/versions.env" ] && . "$ROOT/config/versions.env"
GG_VERSION="${GG_VERSION:-8.10.0}"

TOOLS="$ROOT/tools"
mkdir -p "$TOOLS"

if [ ! -x "$TOOLS/apache-jmeter-$JMETER_VERSION/bin/jmeter" ]; then
  echo ">> Downloading JMeter $JMETER_VERSION"
  curl -fSL "https://archive.apache.org/dist/jmeter/binaries/apache-jmeter-$JMETER_VERSION.tgz" -o "$TOOLS/jmeter.tgz"
  tar -xzf "$TOOLS/jmeter.tgz" -C "$TOOLS" && rm "$TOOLS/jmeter.tgz"
fi

LIB="$TOOLS/apache-jmeter-$JMETER_VERSION/lib"
MANIFEST="$TOOLS/gg-driver-jars.txt"
if [ ! -f "$LIB/ignite-core-$GG_VERSION.jar" ]; then
  echo ">> Installing GridGain JDBC driver $GG_VERSION + dependencies (maven)"
  # remove exactly what a previous driver install added
  [ -f "$MANIFEST" ] && while read -r j; do rm -f "$LIB/$j"; done < "$MANIFEST"
  ls "$LIB" | sort > "$TOOLS/.before"
  mvn -q -f "$ROOT/framework/deps/pom.xml" -Dgg.version="$GG_VERSION" dependency:copy-dependencies \
      -DoutputDirectory="$LIB" -DincludeScope=runtime -DexcludeGroupIds=org.slf4j,ch.qos.logback
  ls "$LIB" | sort | comm -13 "$TOOLS/.before" - > "$MANIFEST"; rm -f "$TOOLS/.before"
fi
echo "JMeter home: $TOOLS/apache-jmeter-$JMETER_VERSION  (GridGain driver $GG_VERSION)"
