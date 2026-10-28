import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Independent availability probe, run next to JMeter (java -cp "<jmeter>/lib/*" Probe.java ...).
 * Every interval, for each target (each node on its own, plus the multi-endpoint "cluster" URL) it opens a FRESH
 * connection and does one PK read and one PK write with hard timeouts. Because it shares nothing with JMeter's
 * connection pool, it separates "the cluster/node is unavailable" from "JMeter's pooled connections are stuck".
 *
 * args: out.csv  pid-to-follow  env.properties  profile.properties
 * env keys:     probe_targets=node1,node2,node3,cluster   probe_url_<name>=jdbc:ignite:thin://...
 * profile keys: probe_interval_ms (1000)  probe_timeout_sec (5)  oltp_rows (1000000)
 * Output csv:   ts_ms,target,op,ok,latency_ms,error
 */
public class Probe {
    static Properties load(String p) throws IOException {
        Properties pr = new Properties();
        try (Reader r = Files.newBufferedReader(Path.of(p))) { pr.load(r); }
        return pr;
    }

    public static void main(String[] a) throws Exception {
        PrintWriter out = new PrintWriter(new FileWriter(a[0], false), true);
        long pid = Long.parseLong(a[1]);
        Properties env = load(a[2]), prof = load(a[3]);
        long interval = Long.parseLong(prof.getProperty("probe_interval_ms", "1000"));
        int tmo = Integer.parseInt(prof.getProperty("probe_timeout_sec", "5"));
        long rows = Long.parseLong(prof.getProperty("oltp_rows", "1000000"));
        out.println("ts_ms,target,op,ok,latency_ms,error");
        List<Thread> ts = new ArrayList<>();
        int slot = 0;
        for (String name : env.getProperty("probe_targets", "").split(",")) {
            name = name.trim();
            if (name.isEmpty()) continue;
            String url = env.getProperty("probe_url_" + name).replace("\\,", ",")
                    + "?queryTimeout=" + tmo + "&connectionTimeout=" + (tmo * 1000);
            final String n = name; final int s = slot++;
            Thread t = new Thread(() -> loop(n, url, pid, interval, tmo, rows, s, out), "probe-" + n);
            t.start(); ts.add(t);
        }
        for (Thread t : ts) t.join();
        out.close();
        System.exit(0);   // Ignite client threads are non-daemon
    }

    static void loop(String name, String url, long pid, long interval, int tmo, long rows, int slot, PrintWriter out) {
        ExecutorService ex = Executors.newCachedThreadPool(r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });
        long i = 0;
        while (ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false)) {
            long start = System.currentTimeMillis();
            long readId = (i * 7L) % 64;                  // sweep ids so reads hit many partitions
            long writeId = (i % 16) * 4 + slot;           // per-target ids, no write conflicts between probe threads
            for (String op : new String[]{"read", "write"}) {
                long t0 = System.nanoTime();
                Future<String> f = ex.submit(() -> {
                    try (Connection c = DriverManager.getConnection(url)) {
                        if (op.equals("read")) {
                            try (PreparedStatement ps = c.prepareStatement("SELECT v FROM probe_kv WHERE id = ?")) {
                                ps.setLong(1, readId); ps.setQueryTimeout(tmo);
                                try (ResultSet rs = ps.executeQuery()) { rs.next(); }
                            }
                        } else {
                            int n;
                            try (PreparedStatement ps = c.prepareStatement("UPDATE probe_kv SET v = ? WHERE id = ?")) {
                                ps.setString(1, "probe-" + System.currentTimeMillis()); ps.setLong(2, writeId); ps.setQueryTimeout(tmo);
                                n = ps.executeUpdate();
                            }
                            if (n == 0) {
                                try (PreparedStatement ps = c.prepareStatement("INSERT INTO probe_kv (id, v) VALUES (?, ?)")) {
                                    ps.setLong(1, writeId); ps.setString(2, "seed"); ps.setQueryTimeout(tmo); ps.executeUpdate();
                                }
                            }
                        }
                        return null;
                    } catch (Throwable e) { return String.valueOf(e.getMessage()).replace(',', ';').replace('\n', ' '); }
                });
                String err;
                try { err = f.get(tmo + 5, TimeUnit.SECONDS); }
                catch (TimeoutException e) { f.cancel(true); err = "HANG: no result within " + (tmo + 5) + "s"; }
                catch (Exception e) { err = String.valueOf(e); }
                long ms = (System.nanoTime() - t0) / 1_000_000;
                out.println(start + "," + name + "," + op + "," + (err == null) + "," + ms + "," + (err == null ? "" : err.substring(0, Math.min(err.length(), 160))));
            }
            i++;
            long sleep = interval - (System.currentTimeMillis() - start);
            if (sleep > 0) try { Thread.sleep(sleep); } catch (InterruptedException e) { return; }
        }
        ex.shutdownNow();
    }
}
