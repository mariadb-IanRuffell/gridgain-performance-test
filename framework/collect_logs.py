#!/usr/bin/env python3
"""Collects each node's server log for a finished test when something went wrong, for the report.

usage: collect_logs.py <run_dir> [--since EPOCH --until EPOCH] [--force]

Always writes node-logs/summary.json (node status + why/why not). Only when a trigger fired (or --force) it also
saves, per node:  node-logs/<node>.log.gz  (full log for the test window)  and an excerpt + grouped WARN/ERROR
messages inside summary.json, which framework/report.py renders as the "Node logs" section.

Triggers: failed operations in results.jtl, injected faults (chaos.json), unplanned node exit, a breakpoint,
failed availability probes, or a node that is not running at the end of the test.

Drivers (env file, same as chaos.py): docker (default) reads `docker logs`; cmd uses `log_cmd` with {node} {since} {until}
(e.g. ssh {node}.internal 'sudo journalctl -u gridgain9 --since @{since} --until @{until} --no-pager').
"""
import calendar, gzip, json, os, re, subprocess, sys, time, collections
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from props import read_props

LINE = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d:\d{3}) \+0000 \[(\w+)\]\[([^\]]*)\]\[([^\]]*)\] ?(.*)$")   # GridGain 9
# GridGain 8 (log4j2 pattern [%d{ISO8601}][%-5p][%t][%c{1}]): [2026-10-28T10:15:30,123][INFO ][main][IgniteKernal%node1] message
LINE8 = re.compile(r"^\[?(\d{4}-\d\d-\d\d[ T]\d\d:\d\d:\d\d),(\d{3})\]\[(\w+)\s*\]\[([^\]]*)\]\[([^\]]*)\](?:\[[^\]]*\])? ?(.*)$")
MAX_ERR, MAX_WARN, MAX_CTX, MAX_LINE = 500, 500, 500, 400
CTX_SECS = 20


def sh(cmd, timeout=300):
    p = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout, errors="replace")
    return p.returncode, p.stdout + p.stderr


def epoch(ts):
    return calendar.timegm(time.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")) + int(ts[20:23]) / 1000.0


def norm(msg):
    msg = re.sub(r"\b[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}\b", "<uuid>", msg)
    msg = re.sub(r"TraceId:[0-9a-f]+", "TraceId:<id>", msg)
    msg = re.sub(r"\d+", "#", msg)
    return msg[:160]


def parse(text):
    """-> list of dict(t, level, thread, cls, msg, more) ; continuation lines (stack traces) attach to the previous entry."""
    out = []
    for raw in text.splitlines():
        m = LINE.match(raw)
        m8 = None if m else LINE8.match(raw)
        if m:
            out.append(dict(t=epoch(m.group(1)), level=m.group(2).upper(), thread=m.group(3), cls=m.group(4), msg=m.group(5), more=[]))
        elif m8:
            out.append(dict(t=calendar.timegm(time.strptime(m8.group(1).replace("T", " "), "%Y-%m-%d %H:%M:%S")) + int(m8.group(2)) / 1000.0,
                            level=m8.group(3).upper(), thread=m8.group(4), cls=m8.group(5).split("%")[0], msg=m8.group(6), more=[]))
        elif out and raw.strip() and len(out[-1]["more"]) < 8:
            out[-1]["more"].append(raw.strip()[:MAX_LINE])
    return out


class Driver:
    def __init__(self, env):
        self.env = env
        self.kind = env.get("chaos_driver", "docker") if not env.get("log_cmd") else "cmd"
        self.prefix = env.get("chaos_container_prefix", "gg9-")

    def status(self, node):
        if self.kind != "docker":
            return dict(status="unknown")
        rc, out = sh("docker inspect -f '{{.State.Status}}|{{.State.ExitCode}}|{{.State.OOMKilled}}|{{.State.StartedAt}}|{{.State.FinishedAt}}' "
                     + self.prefix + node, 30)
        if rc != 0:
            return dict(status="missing")
        st, code, oom, started, finished = (out.strip().split("|") + [""] * 5)[:5]
        return dict(status=st, exit_code=int(code or 0), oom_killed=oom == "true", started=started, finished=finished)

    def log(self, node, since, until):
        if self.kind == "docker":
            return sh(f"docker logs --since {int(since)} --until {int(until)} {self.prefix}{node} 2>&1", 600)[1]
        cmd = self.env["log_cmd"].replace("{node}", node).replace("{since}", str(int(since))).replace("{until}", str(int(until)))
        return sh(cmd, 600)[1]


def triggers(run, nodes_status):
    t = []
    jtl = os.path.join(run, "results.jtl")
    if os.path.exists(jtl):
        n = 0
        with open(jtl, errors="replace") as f:
            for line in f:
                if ",false," in line:
                    n += 1
        if n:
            t.append(f"{n:,} failed operations")
    if os.path.exists(os.path.join(run, "chaos.json")):
        ev = json.load(open(os.path.join(run, "chaos.json"))).get("events", [])
        if ev:
            t.append("fault injection run")
        if any(e["action"] == "unplanned-exit" for e in ev):
            t.append("unplanned node exit")
    if os.path.exists(os.path.join(run, "breakpoint.json")):
        t.append("breakpoint reached")
    probe = os.path.join(run, "probe.csv")
    if os.path.exists(probe):
        if sum(1 for l in open(probe, errors="replace") if ",false," in l):
            t.append("availability probe failures")
    for n, st in nodes_status.items():
        if st.get("status") not in ("running", "unknown"):
            t.append(f"{n} not running at end ({st.get('status')}, exit {st.get('exit_code', '?')})")
    return t


def key_times(run):
    ts = []
    p = os.path.join(run, "chaos.json")
    if os.path.exists(p):
        ts += [e["t_epoch"] for e in json.load(open(p)).get("events", [])]
    return ts


def summarise(entries, marks):
    groups = collections.OrderedDict()
    for e in entries:
        if e["level"] in ("ERROR", "WARN", "WARNING", "FATAL", "SEVERE"):
            k = (e["level"], e["cls"], norm(e["msg"]))
            g = groups.setdefault(k, dict(level=e["level"], cls=e["cls"], count=0, first=e["t"], last=e["t"], example=e["msg"][:MAX_LINE]))
            g["count"] += 1
            g["last"] = e["t"]
    top = sorted(groups.values(), key=lambda g: (g["level"] not in ("ERROR", "FATAL", "SEVERE"), -g["count"]))[:30]
    errs = [e for e in entries if e["level"] in ("ERROR", "FATAL", "SEVERE")]
    warns = [e for e in entries if e["level"] in ("WARN", "WARNING")]

    def sample(lst, cap):
        if len(lst) <= cap:
            return lst
        step = len(lst) / cap
        return [lst[int(i * step)] for i in range(cap)]

    chosen = {id(e): e for e in sample(errs, MAX_ERR)}
    chosen.update({id(e): e for e in sample(warns, MAX_WARN)})
    ctx = [e for e in entries if e["level"] not in ("ERROR", "WARN", "WARNING") and any(abs(e["t"] - m) <= CTX_SECS for m in marks)]
    chosen.update({id(e): e for e in sample(ctx, MAX_CTX)})
    excerpt = [dict(t=round(e["t"], 3), l=e["level"], c=e["cls"], m=e["msg"][:MAX_LINE], x=e["more"][:6]) for e in sorted(chosen.values(), key=lambda e: e["t"])]
    levels = collections.Counter(e["level"] for e in entries)
    return dict(lines=len(entries), levels=dict(levels), groups=top, excerpt=excerpt)


def main(run, since=None, until=None, force=False):
    p = read_props(os.path.join(run, "env.properties"))
    meta = json.load(open(os.path.join(run, "run.json"))) if os.path.exists(os.path.join(run, "run.json")) else {}
    since = since or (meta.get("start_epoch", time.time() - 3600) - 30)
    until = until or (meta.get("end_epoch", time.time()) + 120)
    nodes = [n.strip() for n in p.get("chaos_nodes", "").split(",") if n.strip()]
    if not nodes:
        print("[logs] no chaos_nodes in env file; nothing to collect")
        return
    drv = Driver(p)
    status = {n: drv.status(n) for n in nodes}
    why = triggers(run, status)
    out_dir = os.path.join(run, "node-logs")
    os.makedirs(out_dir, exist_ok=True)
    summary = dict(collected=bool(why or force), reasons=why, window=[since, until], nodes={})
    if why or force:
        marks = key_times(run)
        for n in nodes:
            text = drv.log(n, since, until)
            with gzip.open(os.path.join(out_dir, f"{n}.log.gz"), "wt") as g:
                g.write(text)
            s = summarise(parse(text), marks)
            s["status"] = status[n]
            summary["nodes"][n] = s
        print(f"[logs] collected for {len(nodes)} nodes ({'; '.join(why) or 'forced'})")
    else:
        summary["nodes"] = {n: dict(status=status[n]) for n in nodes}
        print("[logs] clean run - node logs not retained")
    json.dump(summary, open(os.path.join(out_dir, "summary.json"), "w"))


if __name__ == "__main__":
    a = sys.argv[1:]
    main(a[0], float(a[a.index("--since") + 1]) if "--since" in a else None,
         float(a[a.index("--until") + 1]) if "--until" in a else None, "--force" in a)
