"""Workload definitions: schema, data loaders and the per-iteration operation mix.

Everything tunable is read at run time from JMeter properties (-J / profile files), never baked in,
so one generated plan serves steady-state, ramp-to-failure and every cluster size.
NOTE: Groovy snippets must not contain the two-character sequence dollar+brace - JMeter evaluates it
as a function reference before the script compiles.
"""
from dataclasses import dataclass, field
from typing import List

POOL = "gg9"
def tpl(prop, default):
    """GridGain 8 has no zones: tables pick a cache template defined in cluster/local/gridgain-config.xml.
    perf_txn_tpl = TRANSACTIONAL_SNAPSHOT (MVCC, needed for real SQL transactions), perf_tpl = ATOMIC."""
    return 'WITH "template=${__P(%s,%s)}"' % (prop, default)



@dataclass
class Loader:
    table: str
    total_prop: str
    total_default: int
    insert_sql: str
    row_expr: str  # Groovy expression producing a List; `id` (long) and `rnd` are in scope


@dataclass
class Op:
    name: str          # sampler label
    kind: str          # "jdbc" | "groovy"
    sql: str = ""
    args: str = ""
    types: str = ""
    qtype: str = "Prepared Select Statement"
    script: str = ""   # for groovy ops


@dataclass
class Workload:
    key: str
    ddl: List[str]
    drop: List[str]
    loaders: List[Loader]
    pick_script: str
    ops: List[Op]      # position == value of the `op` variable
    defaults: dict = field(default_factory=dict)


# --------------------------------------------------------------------------- OLTP
OLTP_PICK = """
import java.util.concurrent.ThreadLocalRandom
def rnd = ThreadLocalRandom.current()
def gp = { String k, String d -> props.getProperty(k, d) }
long rows = Long.parseLong(gp('oltp_rows', '1000000'))
double skew = Double.parseDouble(gp('key_skew', '1.0'))      // 1 = uniform, >1 = hot-spot
int[] w = [gp('mix_read','60'), gp('mix_update','15'), gp('mix_insert','5'),
           gp('mix_range','10'), gp('mix_tx','10')].collect { Integer.parseInt(it) } as int[]
int roll = rnd.nextInt((int) w.sum()); int op = 0; int acc = 0
for (int i = 0; i < w.length; i++) { acc += w[i]; if (roll < acc) { op = i; break } }
def key = { 1L + (long) (rows * Math.pow(rnd.nextDouble(), skew)) }
long seq = (vars.getObject('seq') ?: 0L) + 1L
vars.putObject('seq', seq)
long salt = Long.parseLong(gp('run_salt', '0'))
vars.put('op', String.valueOf(op))
vars.put('k', String.valueOf(key()))
vars.put('k2', String.valueOf(key()))
vars.put('cust', String.valueOf(1 + rnd.nextInt((int) Math.max(1L, rows.intdiv(10L)))))
vars.put('delta', String.valueOf(1 + rnd.nextInt(100)))
vars.put('newId', String.valueOf(rows + ((salt * 10000L + ctx.getThreadNum()) * 1000000L) + seq))
vars.put('newCust', String.valueOf(1 + rnd.nextInt((int) Math.max(1L, rows.intdiv(10L)))))
vars.put('payload', 'p' + Long.toHexString(rnd.nextLong()) + Long.toHexString(rnd.nextLong()))
SampleResult.setIgnore()
return 'op=' + op
"""

OLTP_TX = """
import org.apache.jmeter.protocol.jdbc.config.DataSourceElement
long a = Long.parseLong(vars.get('k')); long b = Long.parseLong(vars.get('k2'))
long d = Long.parseLong(vars.get('delta'))
if (a == b) { b = a + 1 }
def c = DataSourceElement.getConnection('gg9')
try {
  c.setAutoCommit(false)
  def p1 = c.prepareStatement('UPDATE accounts SET balance = balance - ? WHERE id = ?')
  p1.setLong(1, d); p1.setLong(2, a); p1.executeUpdate()
  def p2 = c.prepareStatement('UPDATE accounts SET balance = balance + ? WHERE id = ?')
  p2.setLong(1, d); p2.setLong(2, b); p2.executeUpdate()
  c.commit()
} catch (Throwable t) {
  try { c.rollback() } catch (Throwable ignored) { }
  throw t
} finally {
  try { c.setAutoCommit(true) } catch (Throwable ignored) { }
  c.close()
}
return 'tx ok'
"""

OLTP = Workload(
    key="oltp",
    ddl=[
        "CREATE TABLE IF NOT EXISTS accounts (id BIGINT PRIMARY KEY, customer_id INT, "
        "balance DECIMAL(18,2), status VARCHAR(16), region VARCHAR(8), payload VARCHAR(200), "
        f"updated_at TIMESTAMP) " + tpl("oltp_template", "perf_txn_tpl"),
        "CREATE INDEX IF NOT EXISTS accounts_customer_idx ON accounts (customer_id)",
    ],
    drop=["DROP TABLE IF EXISTS accounts"],
    loaders=[Loader(
        table="accounts", total_prop="oltp_rows", total_default=1000000,
        insert_sql="INSERT INTO accounts (id, customer_id, balance, status, region, payload, updated_at) "
                   "VALUES (?, ?, ?, ?, ?, ?, ?)",
        row_expr="[id, (int) (1 + (id % Math.max(1L, total.intdiv(10L)))), new BigDecimal(rnd.nextInt(1000000)).movePointLeft(2), "
                 "['ACTIVE','ACTIVE','ACTIVE','DORMANT','CLOSED'][rnd.nextInt(5)], "
                 "['EU','US','APAC','LATAM'][rnd.nextInt(4)], 'seed-' + id + '-' + 'x' * 100, "
                 "new java.sql.Timestamp(System.currentTimeMillis())]")],
    pick_script=OLTP_PICK,
    ops=[
        Op("oltp.read", "jdbc", "SELECT id, customer_id, balance, status, payload FROM accounts WHERE id = ?",
           "${k}", "BIGINT"),
        Op("oltp.update", "jdbc", "UPDATE accounts SET balance = balance + ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
           "${delta},${k}", "INTEGER,BIGINT", qtype="Prepared Update Statement"),
        Op("oltp.insert", "jdbc",
           "INSERT INTO accounts (id, customer_id, balance, status, region, payload, updated_at) "
           "VALUES (?, ?, 0, 'ACTIVE', 'EU', ?, CURRENT_TIMESTAMP)",
           "${newId},${newCust},${payload}", "BIGINT,INTEGER,VARCHAR", qtype="Prepared Update Statement"),
        Op("oltp.range_by_customer", "jdbc",
           "SELECT id, balance, status FROM accounts WHERE customer_id = ?", "${cust}", "INTEGER"),
        Op("oltp.tx_transfer", "groovy", script=OLTP_TX),
    ],
)

# --------------------------------------------------------------------------- Analytical
ANALYTICAL_PICK = """
import java.util.concurrent.ThreadLocalRandom
def rnd = ThreadLocalRandom.current()
def gp = { String k, String d -> props.getProperty(k, d) }
long orders = Long.parseLong(gp('olap_orders', '2000000'))
int[] w = [gp('mix_agg','30'), gp('mix_join','30'), gp('mix_topn','10'),
           gp('mix_lookup','25'), gp('mix_distinct','5')].collect { Integer.parseInt(it) } as int[]
int roll = rnd.nextInt((int) w.sum()); int op = 0; int acc = 0
for (int i = 0; i < w.length; i++) { acc += w[i]; if (roll < acc) { op = i; break } }
int span = Integer.parseInt(gp('olap_window_days', '30'))
int from = rnd.nextInt(365 - span)
vars.put('op', String.valueOf(op))
vars.put('dayFrom', String.valueOf(from))
vars.put('dayTo', String.valueOf(from + span))
vars.put('cust', String.valueOf(1 + rnd.nextInt((int) Math.max(1L, orders.intdiv(20L)))))
SampleResult.setIgnore()
return 'op=' + op
"""

ANALYTICAL = Workload(
    key="analytical",
    ddl=[
        "CREATE TABLE IF NOT EXISTS customers (id BIGINT PRIMARY KEY, name VARCHAR(40), "
        "segment VARCHAR(16), region VARCHAR(8)) " + tpl("olap_template", "perf_tpl"),
        "CREATE TABLE IF NOT EXISTS products (id BIGINT PRIMARY KEY, category VARCHAR(16), "
        "price DECIMAL(10,2)) " + tpl("olap_template", "perf_tpl"),
        "CREATE TABLE IF NOT EXISTS orders (id BIGINT PRIMARY KEY, customer_id BIGINT, product_id BIGINT, "
        "qty INT, amount DECIMAL(12,2), order_day INT, region VARCHAR(8), status VARCHAR(12)) "
        + tpl("olap_template", "perf_tpl"),
        "CREATE INDEX IF NOT EXISTS orders_customer_idx ON orders (customer_id)",
        "CREATE INDEX IF NOT EXISTS orders_day_idx ON orders (order_day)",
    ],
    drop=["DROP TABLE IF EXISTS orders", "DROP TABLE IF EXISTS products", "DROP TABLE IF EXISTS customers"],
    loaders=[
        Loader("customers", "olap_customers", 100000,
               "INSERT INTO customers (id, name, segment, region) VALUES (?, ?, ?, ?)",
               "[id, 'cust-' + id, ['RETAIL','SMB','ENTERPRISE','PUBLIC'][rnd.nextInt(4)], "
               "['EU','US','APAC','LATAM'][rnd.nextInt(4)]]"),
        Loader("products", "olap_products", 5000,
               "INSERT INTO products (id, category, price) VALUES (?, ?, ?)",
               "[id, 'cat' + (id % 25), new BigDecimal(100 + rnd.nextInt(99900)).movePointLeft(2)]"),
        Loader("orders", "olap_orders", 2000000,
               "INSERT INTO orders (id, customer_id, product_id, qty, amount, order_day, region, status) "
               "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
               "[id, 1L + rnd.nextInt((int) Math.max(1L, total.intdiv(20L))), 1L + rnd.nextInt(5000), 1 + rnd.nextInt(10), "
               "new BigDecimal(100 + rnd.nextInt(500000)).movePointLeft(2), rnd.nextInt(365), "
               "['EU','US','APAC','LATAM'][rnd.nextInt(4)], ['NEW','PAID','SHIPPED','RETURNED'][rnd.nextInt(4)]]"),
    ],
    pick_script=ANALYTICAL_PICK,
    ops=[
        Op("olap.agg_by_region", "jdbc",
           "SELECT region, COUNT(*) c, SUM(amount) s FROM orders WHERE order_day BETWEEN ? AND ? GROUP BY region",
           "${dayFrom},${dayTo}", "INTEGER,INTEGER"),
        Op("olap.join_segment_category", "jdbc",
           "SELECT c.segment, p.category, SUM(o.amount) s FROM orders o JOIN customers c ON o.customer_id = c.id "
           "JOIN products p ON o.product_id = p.id WHERE o.order_day BETWEEN ? AND ? GROUP BY c.segment, p.category",
           "${dayFrom},${dayTo}", "INTEGER,INTEGER"),
        Op("olap.topn_customers", "jdbc",
           "SELECT customer_id, SUM(amount) s FROM orders GROUP BY customer_id ORDER BY s DESC LIMIT 10"),
        Op("olap.customer_history", "jdbc",
           "SELECT id, amount, order_day, status FROM orders WHERE customer_id = ? ORDER BY order_day DESC LIMIT 50",
           "${cust}", "BIGINT"),
        Op("olap.count_distinct", "jdbc",
           "SELECT COUNT(DISTINCT customer_id) FROM orders WHERE order_day BETWEEN ? AND ?",
           "${dayFrom},${dayTo}", "INTEGER,INTEGER"),
    ],
)

# --------------------------------------------------------------------------- Bulk ingest
INGEST_PICK = """
import java.util.concurrent.ThreadLocalRandom
def rnd = ThreadLocalRandom.current()
def gp = { String k, String d -> props.getProperty(k, d) }
int single = Integer.parseInt(gp('mix_ingest_single', '10'))
int op = rnd.nextInt(100) < single ? 1 : 0
long seq = (vars.getObject('seq') ?: 0L) + 1L
vars.putObject('seq', seq)
long salt = Long.parseLong(gp('run_salt', '0'))
vars.put('op', String.valueOf(op))
vars.put('base', String.valueOf(((salt * 10000L + ctx.getThreadNum()) * 1000000000L) + seq * 10000L))
SampleResult.setIgnore()
return 'op=' + op
"""

INGEST_BATCH = """
import java.util.concurrent.ThreadLocalRandom
import org.apache.jmeter.protocol.jdbc.config.DataSourceElement
def rnd = ThreadLocalRandom.current()
int n = Integer.parseInt(props.getProperty('ingest_batch', '500'))
long base = Long.parseLong(vars.get('base'))
def c = DataSourceElement.getConnection('gg9')
try {
  def ps = c.prepareStatement('INSERT INTO events (id, device_id, ts, metric, val, tags) VALUES (?, ?, ?, ?, ?, ?)')
  for (int i = 0; i < n; i++) {
    ps.setLong(1, base + i); ps.setInt(2, rnd.nextInt(100000))
    ps.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis()))
    ps.setString(4, 'metric' + rnd.nextInt(50)); ps.setDouble(5, rnd.nextDouble() * 100)
    ps.setString(6, 'tag-' + rnd.nextInt(1000) + '-' + 'x' * 60)
    ps.addBatch()
  }
  ps.executeBatch()
} finally { c.close() }
return 'batch ' + n
"""

INGEST = Workload(
    key="ingest",
    ddl=[
        "CREATE TABLE IF NOT EXISTS events (id BIGINT PRIMARY KEY, device_id INT, ts TIMESTAMP, "
        "metric VARCHAR(32), val DOUBLE, tags VARCHAR(100)) " + tpl("ingest_template", "perf_tpl"),
    ],
    drop=["DROP TABLE IF EXISTS events"],
    loaders=[],
    pick_script=INGEST_PICK,
    ops=[
        Op("ingest.batch", "groovy", script=INGEST_BATCH),
        Op("ingest.single", "jdbc",
           "INSERT INTO events (id, device_id, ts, metric, val, tags) VALUES (?, 1, CURRENT_TIMESTAMP, 'm', 1.0, 't')",
           "${base}", "BIGINT", qtype="Prepared Update Statement"),
    ],
)

WORKLOADS = {w.key: w for w in (OLTP, ANALYTICAL, INGEST)}
