#!/usr/bin/env python3
"""Generates the JMeter plans in ./plans from framework/workloads.py.

  plans/schema.jmx         create zone + tables for every workload
  plans/drop.jmx           drop tables
  plans/load-<wl>.jmx      bulk-load seed data (oltp, analytical)
  plans/run-<wl>.jmx       the measured workload (steady-state AND ramp-to-failure; shaped by properties)
"""
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import jmx  # noqa: E402
from workloads import WORKLOADS, POOL  # noqa: E402

OUT = pathlib.Path(__file__).resolve().parent.parent / "plans"
DRIVER = "org.apache.ignite.IgniteJdbcThinDriver"
URL = "${__P(jdbc_url,jdbc:ignite:thin://localhost:10800\\,localhost:10801\\,localhost:10802)}"
URL += "?queryTimeout=${__P(query_timeout_sec,0)}&connectionTimeout=${__P(connect_timeout_ms,0)}"
LOAD_SCRIPT = """
import java.util.concurrent.ThreadLocalRandom
import org.apache.jmeter.protocol.jdbc.config.DataSourceElement
long total = Long.parseLong(props.getProperty('%(prop)s', '%(default)s'))
int batch = Integer.parseInt(props.getProperty('load_batch', '1000'))
long it = (vars.getObject('it') ?: 0L) + 1L
vars.putObject('it', it)
long n = (it - 1L) * Long.parseLong(props.getProperty("load_threads", "8")) + ctx.getThreadNum()   // deterministic, no shared counter
long start = n * batch + 1L
if (start > total) { SampleResult.setIgnore(); SampleResult.setStopThread(true); return 'done' }
long end = Math.min(total, start + batch - 1L)
def rnd = ThreadLocalRandom.current()
def c = DataSourceElement.getConnection('gg9')
try {
  def ps = c.prepareStatement('%(sql)s')
  for (long id = start; id <= end; id++) {
    def row = %(row)s
    for (int i = 0; i < row.size(); i++) { ps.setObject(i + 1, row[i]) }
    ps.addBatch()
  }
  ps.executeBatch()
} finally { c.close() }
return 'rows ' + (end - start + 1L)
"""


def write(name, plan):
    OUT.mkdir(exist_ok=True)
    (OUT / name).write_text(jmx.render_plan(plan))
    print("wrote", OUT / name)


def base_plan(title, variables=None):
    return jmx.test_plan(title, variables or {})


def pool(max_expr):
    return jmx.jdbc_config("GridGain JDBC pool", POOL, DRIVER, URL, max_expr)


def ddl_plan(title, statements):
    tp = base_plan(title)
    tp.add(pool("4"))
    tg = jmx.setup_thread_group("DDL")
    for i, stmt in enumerate(statements):
        tg.add(jmx.jdbc_sampler(f"ddl-{i:02d}", POOL, stmt, qtype="Update Statement"))
    tp.add(tg)
    return tp


def schema_and_drop():
    seen, create = set(), []
    for wl in WORKLOADS.values():
        for s in wl.ddl:
            if s not in seen:
                seen.add(s)
                create.append(s)
    create.append("CREATE TABLE IF NOT EXISTS probe_kv (id BIGINT PRIMARY KEY, v VARCHAR(64)) WITH \"template=perf_tpl\"")  # availability probe
    write("schema.jmx", ddl_plan("GG9 schema", create))
    drops = [s for wl in WORKLOADS.values() for s in wl.drop] + ["DROP TABLE IF EXISTS probe_kv"]
    write("drop.jmx", ddl_plan("GG9 drop", drops))


def load_plan(wl):
    tp = base_plan(f"GG9 load {wl.key}")
    tp.add(pool("${__P(load_threads,8)}"))
    for ld in wl.loaders:
        tg = jmx.thread_group(f"load {ld.table}", "${__P(load_threads,8)}", "1", loops=-1)
        script = LOAD_SCRIPT % dict(prop=ld.total_prop, default=ld.total_default,
                                    sql=ld.insert_sql, row=ld.row_expr)
        tg.add(jmx.jsr223_sampler(f"load.{ld.table}", script))
        tp.add(tg)
    write(f"load-{wl.key}.jmx", tp)


TC_PRELUDE = """
import java.sql.*
def jdbcUrl = props.getProperty('jdbc_url', 'jdbc:ignite:thin://localhost:10800,localhost:10801,localhost:10802') +
    '?queryTimeout=' + props.getProperty('query_timeout_sec', '0') + '&connectionTimeout=' + props.getProperty('connect_timeout_ms', '0')
// one connection per virtual user, opened lazily (so a ramp opens connections gradually) and replaced after any failure
def getConn = {
  def c = vars.getObject('conn')
  if (c == null || c.isClosed()) {
    c = DriverManager.getConnection(jdbcUrl); vars.putObject('conn', c)
    def reg = props.get('gg9.conns')
    if (reg == null) { props.putIfAbsent('gg9.conns', new java.util.concurrent.ConcurrentLinkedQueue()); reg = props.get('gg9.conns') }
    reg.add(c)
  }
  return c
}
def discardConn = { def c = vars.getObject('conn'); vars.putObject('conn', null); try { c?.close() } catch (Throwable ignored) { }
  try { props.get('gg9.conns')?.remove(c) } catch (Throwable ignored) { } }
def release = { c -> }
"""
TYPE_SETTERS = {"BIGINT": "setLong(%d, Long.parseLong(%s))", "INTEGER": "setInt(%d, Integer.parseInt(%s))",
                "VARCHAR": "setString(%d, %s)"}


CLOSE_ALL = """
def reg = props.get('gg9.conns'); int n = 0
if (reg != null) { def c; while ((c = reg.poll()) != null) { try { c.close(); n++ } catch (Throwable ignored) { } } }
return 'closed ' + n
"""


def tc_script(op):
    """Groovy version of an op that uses the per-thread connection instead of JMeter's pool."""
    if op.kind == "jdbc":
        names = re.findall(r"\$\{(\w+)\}", op.args)
        types = [t.strip() for t in op.types.split(",") if t.strip()]
        sets = "".join("    ps.%s\n" % (TYPE_SETTERS[t] % (i + 1, "vars.get('%s')" % n)) for i, (n, t) in enumerate(zip(names, types)))
        read = "Select" in op.qtype
        body = ("def c = getConn()\ndef ps = c.prepareStatement('''" + op.sql + "''')\ntry {\n" + sets +
                ("    int n = 0\n    def rs = ps.executeQuery()\n    try { while (rs.next()) { n++ } } finally { rs.close() }\n" if read
                 else "    int n = ps.executeUpdate()\n") + "    return 'rows ' + n\n} finally { ps.close() }\n")
        hoisted = ""
    else:
        lines = op.script.splitlines()
        hoisted = "\n".join(l for l in lines if l.strip().startswith("import "))
        body = "\n".join(l for l in lines if not l.strip().startswith("import ")) \
            .replace("DataSourceElement.getConnection('gg9')", "getConn()").replace("c.close()", "release(c)")
    return hoisted + "\n" + TC_PRELUDE + "try {\n" + body + "\n} catch (Throwable t) {\n  discardConn()\n  throw t\n}\n"


def run_plan(wl, thread_conn=False):
    tp = base_plan(f"GG9 run {wl.key}" + (" (per-thread connections)" if thread_conn else ""))
    if not thread_conn:
        tp.add(pool("${__intSum(${__P(threads,32)},8)}"))
    # threads/rampup/duration come from properties: steady-state => ramp is short, threads fixed;
    # ramp-to-failure => threads = ceiling, rampup = long, a watcher stops the run at the breakpoint.
    tg = jmx.thread_group("workload", "${__P(threads,32)}", "${__P(rampup,60)}",
                          duration="${__P(duration,600)}", delay="${__P(startup_delay,0)}")
    tg.add(jmx.jsr223_sampler("pick-op", wl.pick_script))
    sw = jmx.switch_controller("op-mix", "${op}")
    for op in wl.ops:
        if thread_conn:
            sw.add(jmx.jsr223_sampler(op.name, tc_script(op)))
        elif op.kind == "jdbc":
            sw.add(jmx.jdbc_sampler(op.name, POOL, op.sql, qtype=op.qtype, args=op.args, types=op.types))
        else:
            sw.add(jmx.jsr223_sampler(op.name, op.script))
    tg.add(sw)
    tg.add(jmx.constant_timer("think-time", "${__P(think_ms,0)}"))
    tp.add(tg)
    if thread_conn:
        td = jmx.teardown_thread_group("close connections")
        td.add(jmx.jsr223_sampler("teardown.close_connections", CLOSE_ALL))
        tp.add(td)
    write(f"run-{wl.key}{'-tc' if thread_conn else ''}.jmx", tp)


if __name__ == "__main__":
    schema_and_drop()
    for w in WORKLOADS.values():
        if w.loaders:
            load_plan(w)
        run_plan(w)
        run_plan(w, thread_conn=True)
