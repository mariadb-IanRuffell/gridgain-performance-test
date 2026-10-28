# GridGain 8.10 performance test framework (branch `gg-8.10`; the GridGain 9 version is on `main`)

JMeter-based, JDBC-driven. Two test shapes over three workloads, with an HTML report per run.

| | steady-state (`mode=steady`) | ramp-to-failure (`mode=ramp`) |
|---|---|---|
| goal | stable latency/throughput over a set period | find the concurrency where the SLO breaks |
| shape | N threads, short warm-up, fixed duration | threads ramp linearly to a ceiling; a watchdog stops the run when error rate or p95 breaches the SLO for N consecutive windows |
| output | steady-window percentiles, time series | breakpoint, saturation curve (ops/s and p95 vs threads) |

Workloads (`framework/workloads.py`): **oltp** (point read/update/insert, indexed range, 2-row transaction; Zipf-style key skew), **analytical** (aggregations, 3-way join, top-N, count distinct, indexed lookup), **ingest** (JDBC batch inserts + singles).

## GridGain 8.10 specifics (this branch)
- **Edition / license:** with a GridGain 8 license file (`gridgain-license.xml` in the project root, or `GG_LICENSE_FILE`) the cluster runs `gridgain/ultimate:8.10.0`. Without one it falls back to `gridgain/community:8.10.0-b2-openjdk21` (the only community 8.10 build so far - a beta) and says so. A GridGain 9 license (`.json`) does not work for 8.
- **Cluster:** 3 nodes from `cluster/local/docker-compose.yml`; `gridgain-config.xml` enables persistence (so `cluster-up.sh` activates the cluster) and defines the cache templates the SQL tables use.
- **Client:** thin JDBC driver (`org.apache.ignite.IgniteJdbcThinDriver`, inside `ignite-core`), URL `jdbc:ignite:thin://h1:10800,h2:10800,...` (random endpoint + failover). Ignite 2.x needs the JDK 17+ `--add-opens` flags in `config/jvm-opens.args` (applied to JMeter and the probe automatically). Timeouts are the URL params `queryTimeout` (s) and `connectionTimeout` (ms).
- **Tables / transactions:** there are no zones; tables are created `WITH "template=..."`. The OLTP `accounts` table uses `perf_txn_tpl` (`TRANSACTIONAL_SNAPSHOT`, i.e. MVCC), the only mode with real SQL transactions in 8.x, so `oltp.tx_transfer` is a genuine 2-row transaction as on GridGain 9. Analytical and ingest tables use the faster `perf_tpl` (`ATOMIC`). Override with `oltp_template` / `olap_template` / `ingest_template`.
- **Chaos:** node readiness is "REST answers and the cluster is active" (`chaos_ready_*` in the env file). With persistence a restarted node rejoins the baseline topology automatically.

## Quick start (local)

```bash
./scripts/setup-tools.sh                 # JMeter 5.6.3 + GridGain 8 JDBC driver into ./tools
cp /path/to/gridgain-license.xml .       # optional: GridGain 8 license => Ultimate edition (else Community beta)
./scripts/cluster-up.sh                  # 3-node docker cluster, initialised
python3 framework/gen_plans.py           # (re)generate plans/*.jmx after editing workloads
./scripts/prepare-data.sh oltp local smoke   # schema + seed data
./scripts/run-test.sh smoke local        # 60 s end-to-end check -> results/<run>/report.html
./scripts/run-test.sh oltp-steady local
./scripts/run-test.sh oltp-ramp local
```

## Chaos / fault-injection tests
Any profile with `chaos_events=<offset-sec>:<action>:<node>,...` runs `framework/chaos.py` alongside JMeter. Offsets count from the first recorded sample; actions are `stop` (graceful), `kill` (SIGKILL), `pause`/`unpause` (freeze), `start`, `restart`. After each start the injector polls the node until it reports STARTED and records the rejoin time; at the end of the run it restores every node it touched.

| profile | fault |
|---|---|
| `chaos-oltp-node-stop` | graceful stop of 1 node at 5 min, back at 8 min |
| `chaos-oltp-node-kill` | SIGKILL 1 node, restart |
| `chaos-oltp-node-pause` | freeze a node for 2 min (GC-stall / partition-like) |
| `chaos-oltp-rolling-restart` | restart node1, node2, node3 in turn |
| `chaos-oltp-quorum-loss` | lose 2 of 3 nodes (hard outage), then recover |
| `chaos-ingest-node-kill` | crash a node during batch ingest |

```bash
./scripts/run-test.sh chaos-oltp-node-kill local
```
The report gains a **Fault injection** section (event timeline, baseline / disruption / after-recovery phase stats, longest full outage, time back to 90% of baseline) and numbered fault markers on every time chart. Cloud: set `chaos_driver=cmd` and the `chaos_cmd_*` templates in the env file (ssh/systemctl, kubectl, SSM). Chaos profiles set `pool_check_query=SELECT 1` so JMeter evicts connections that died with the node; this adds a round trip per operation, so compare chaos runs to a baseline taken with the same setting.

## Node logs
When a test has failed operations, injected faults, an unplanned node exit, a breakpoint or probe failures, `framework/collect_logs.py` saves every node\'s server log for the test window (`results/<run>/node-logs/<node>.log.gz`) and the report gains a **Node logs** section: node state at the end (running / exited + exit code / OOM), counts of errors and warnings, grouped distinct messages, and a filterable viewer (errors, warnings, ±20 s of context around each fault, jump-to-fault buttons). Clean runs keep nothing. Docker clusters are read with `docker logs`; for cloud set `log_cmd` in the env file (e.g. `ssh {node}.internal 'sudo journalctl -u gridgain9 --since @{since} --until @{until} --no-pager'`).

## Layout
- `profiles/*.properties` – one file per test: workload, mode, threads/ramp/duration, op mix, SLO stop criteria.
- `config/env/*.properties` – where it runs (JDBC URL, zone sizing, resource collector). Add `cloud.properties` from the example.
- `config/defaults.properties` – dataset sizes. Profiles override; **the same sizes must be used for load and run**.
- `framework/` – plan generator, ramp watchdog, report builder. `plans/` – generated JMX (committed output, regenerate don't hand-edit).
- `results/<run>/` – `results.jtl`, `report.html`, `summary.json`, `breakpoint.json`, `resources.csv`, copies of the inputs.

## Tuning a profile
All knobs are JMeter properties read at runtime: `threads`, `rampup`, `duration`, `think_ms`, `mix_*`, `key_skew`, `ingest_batch`, `stop_error_pct`, `stop_p95_ms`, `stop_consecutive`, `window_sec`. Override any on the command line by editing the profile; `run_id` salt keeps inserted keys unique across reruns.

## Moving to cloud
1. Provision cluster nodes and a **separate load-generator VM** in the same AZ (JMeter must not share a host with a node; size it so its CPU stays <60%). Keep JMeter's heap (`JMETER_HEAP`) large for high thread counts.
2. Copy `config/env/cloud.properties.example` → `cloud.properties`; set JDBC endpoints, `zone_partitions`, node spec text, `collector=none` (use CloudWatch/Prometheus for node metrics and attach later).
3. `./scripts/prepare-data.sh all cloud` then `./scripts/run-test.sh <profile> cloud`. For more load than one JMeter host can generate, use JMeter distributed mode (planned next).
4. Dataset should exceed RAM-resident size if you intend to measure persistence paths; scale `oltp_rows` / `olap_orders` accordingly.
