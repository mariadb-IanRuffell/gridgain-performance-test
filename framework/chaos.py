#!/usr/bin/env python3
"""Fault injector that runs alongside a JMeter test.

usage: chaos.py <out_dir> <profile.props> <env.props> <jmeter_pid>

Profile:  chaos_events=300:stop:node3,480:start:node3     (offset-seconds:action:node, comma separated)
          actions: stop (graceful) | kill (SIGKILL) | start | restart | pause (freeze) | unpause
Offsets are measured from the first recorded JMeter sample, so they line up with the report's time axis.

Env drivers (chaos_driver):
  docker  containers are <chaos_container_prefix><node>; readiness polled on REST port chaos_rest_port_base + (N-1)
  cmd     shell templates with {node}: chaos_cmd_stop / _kill / _start / _pause / _unpause  (ssh, kubectl, systemctl, aws ssm ...)
          readiness: chaos_cmd_ready (exit 0 == node is back in the topology), optional.

Writes chaos.json (every event with real epoch timestamps, rejoin times, failures). Always tries to restore
every node it touched when the run ends so the cluster is left healthy.
"""
import json, os, re, subprocess, sys, threading, time, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from props import read_props

lock = threading.Lock()
DOWN = set()   # nodes we have deliberately taken down
EVENTS = []
READY_TIMEOUT = 600


def sh(cmd, timeout=120):
    p = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    return p.returncode, (p.stdout + p.stderr).strip()


class Docker:
    def __init__(self, env):
        self.prefix = env.get("chaos_container_prefix", "gg9-")
        self.port_base = int(env.get("chaos_rest_port_base", 10300))
        self.stop_grace = env.get("chaos_stop_grace_sec", "30")
        self.rest_port = env.get("chaos_rest_container_port", "10300")             # container-side REST port (GG9: 10300, GG8: 8080)
        self.ready_path = env.get("chaos_ready_path", "/management/v1/node/state")
        self.ready_match = env.get("chaos_ready_match", "STARTED")

    def name(self, node):
        return self.prefix + node

    def run(self, action, node):
        c = self.name(node)
        cmd = {"stop": f"docker stop -t {self.stop_grace} {c}", "kill": f"docker kill {c}",
               "start": f"docker start {c}", "pause": f"docker pause {c}", "unpause": f"docker unpause {c}"}[action]
        return sh(cmd, 180)

    def ready(self, node):
        n = int(re.sub(r"\D", "", node) or 1)
        try:
            with urllib.request.urlopen(f"http://localhost:{self.port_base + n - 1}{self.ready_path}", timeout=3) as r:
                return r.status == 200 and self.ready_match in r.read().decode()
        except Exception:
            return False

    def status(self, node):
        rc, out = sh(f"docker inspect -f '{{{{.State.Status}}}}' {self.name(node)}", 20)
        return out if rc == 0 else "missing"

    def ports_bound(self, node):
        rc, out = sh(f"docker port {self.name(node)} {self.rest_port}", 20)
        return rc == 0 and bool(out.strip())

    def restore(self, nodes):
        for n in nodes:
            sh(f"docker unpause {self.name(n)}"); sh(f"docker start {self.name(n)}")
            if not self.ports_bound(n):          # Docker Desktop sometimes drops host port bindings after stop/start
                sh(f"docker restart {self.name(n)}", 180)


class Cmd:
    def __init__(self, env):
        self.env = env

    def run(self, action, node):
        t = self.env.get(f"chaos_cmd_{action}")
        if not t:
            return 1, f"chaos_cmd_{action} not configured"
        return sh(t.replace("{node}", node), 300)

    def ready(self, node):
        t = self.env.get("chaos_cmd_ready")
        return bool(t) and sh(t.replace("{node}", node), 60)[0] == 0 if t else True

    def status(self, node):
        t = self.env.get("chaos_cmd_status")  # optional: must print running|stopped|paused
        return sh(t.replace("{node}", node), 30)[1].strip() if t else "running"

    def ports_bound(self, node):
        return True

    def restore(self, nodes):
        for n in nodes:
            self.run("unpause", n); self.run("start", n)


def parse(spec):
    out = []
    for item in filter(None, (x.strip() for x in spec.split(","))):
        off, action, node = item.split(":")
        out.append((float(off), action, node))
    return sorted(out)


def record(out_dir, **ev):
    with lock:
        EVENTS.append(ev)
        with open(os.path.join(out_dir, "chaos.json"), "w") as f:
            json.dump({"events": sorted(EVENTS, key=lambda e: e["t_epoch"])}, f, indent=2)


def fire(drv, out_dir, t_start, offset, action, node, alive):
    while time.time() < t_start + offset:
        if not alive():
            return
        time.sleep(min(1, max(0, t_start + offset - time.time())))
    if not alive():
        return
    todo = ["stop", "start"] if action == "restart" else [action]
    for a in todo:
        t = time.time()
        if a in ("stop", "kill", "pause"):
            with lock:
                DOWN.add(node)
        rc, msg = drv.run(a, node)
        ev = dict(t_epoch=t, planned_offset=offset, action=a, node=node, ok=rc == 0, note=msg[:200])
        print(f"[chaos] +{t - t_start:6.1f}s {a} {node} -> {'ok' if rc == 0 else 'FAILED: ' + msg[:100]}", flush=True)
        record(out_dir, **ev)
        if a in ("start", "unpause") and rc == 0:
            with lock:
                DOWN.discard(node)
            t_back = None
            deadline = time.time() + READY_TIMEOUT
            while time.time() < deadline:
                if drv.ready(node):
                    t_back = time.time(); break
                time.sleep(2)
            record(out_dir, t_epoch=t_back or time.time(), planned_offset=offset, action="ready" if t_back else "ready-timeout",
                   node=node, ok=bool(t_back), note=f"{(t_back - t):.1f}s after {a}" if t_back else "node not ready")
            print(f"[chaos] {node} {'ready after %.1fs' % (t_back - t) if t_back else 'NOT READY'}", flush=True)
        if a == "stop" and action == "restart":
            time.sleep(float(os.environ.get("CHAOS_RESTART_GAP", 10)))


def preflight(env):
    """Refuse to start a chaos run on an unhealthy cluster (a node already down invalidates the experiment)."""
    drv = Docker(env) if env.get("chaos_driver", "docker") == "docker" else Cmd(env)
    nodes = [n.strip() for n in env.get("chaos_nodes", "").split(",") if n.strip()]
    bad = []
    for n in nodes:
        st = drv.status(n)
        ok = st == "running" and drv.ports_bound(n) and drv.ready(n)
        print(f"[preflight] {n}: status={st} ports={'ok' if drv.ports_bound(n) else 'MISSING'} ready={'yes' if ok else 'NO'}")
        if not ok:
            bad.append(n)
    if bad:
        print(f"[preflight] FAILED: {', '.join(bad)} not healthy. Fix the cluster (docker restart / cluster-up.sh) and retry.")
        sys.exit(1)


def watch_unplanned(drv, nodes, out_dir, down, alive):
    """Record nodes that leave 'running' without us asking (e.g. a node stopped itself after a critical failure)."""
    seen = set()
    while alive():
        for n in nodes:
            st = drv.status(n)
            with lock:
                expected = n in down
            if st != "running" and not expected and n not in seen:
                seen.add(n)
                print(f"[chaos] UNPLANNED: {n} is {st}", flush=True)
                record(out_dir, t_epoch=time.time(), planned_offset=-1, action="unplanned-exit", node=n, ok=False,
                       note=f"node became '{st}' without an injected fault - check its logs")
            elif st == "running":
                seen.discard(n)
        time.sleep(2)


def main(out_dir, profile, envf, pid):
    p, env = read_props(profile), read_props(envf)
    spec = p.get("chaos_events")
    if not spec:
        return
    drv = Docker(env) if env.get("chaos_driver", "docker") == "docker" else Cmd(env)
    events = parse(spec)
    jtl = os.path.join(out_dir, "results.jtl")

    def alive():
        try:
            os.kill(pid, 0); return True
        except OSError:
            return False

    while alive() and not (os.path.exists(jtl) and sum(1 for _ in open(jtl)) > 1):
        time.sleep(1)
    t_start = time.time()
    nodes = [n.strip() for n in env.get("chaos_nodes", "").split(",") if n.strip()] or sorted({n for _, _, n in events})
    threading.Thread(target=watch_unplanned, args=(drv, nodes, out_dir, DOWN, alive), daemon=True).start()
    threads = [threading.Thread(target=fire, args=(drv, out_dir, t_start, o, a, n, alive), daemon=True) for o, a, n in events]
    for t in threads:
        t.start()
    while alive():
        time.sleep(1)
    for t in threads:
        t.join(timeout=READY_TIMEOUT + 60)
    drv.restore(sorted({n for _, _, n in events}))  # leave the cluster healthy
    print("[chaos] done; nodes restored", flush=True)


if __name__ == "__main__":
    if sys.argv[1] == "--preflight":
        preflight(read_props(sys.argv[2]))
        sys.exit(0)
    main(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))
