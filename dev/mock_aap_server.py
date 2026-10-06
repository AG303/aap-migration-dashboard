#!/usr/bin/env python3
"""
mock_aap_server.py - Fake AAP 2.4 and AAP 2.6 APIs for trying the dashboard without
touching real systems (and for testing changes to it).

  python3 mock_aap_server.py            # 2.4 on :8424, 2.6 on :8426, token "mock-token"

  export AAP24_HOST=http://127.0.0.1:8424 AAP24_TOKEN=mock-token
  export AAP26_HOST=http://127.0.0.1:8426 AAP26_TOKEN=mock-token
  python3 ../aap_migration_dashboard.py -o /tmp/demo --serve 8080

Data is generated deterministically: ~30 orgs, ~300 users, templates with a mix of
daily / weekly / monthly / hourly / manual-only schedules. 2.6 holds a partial,
overlapping copy so every migration status appears in the dashboard.
"""
import argparse
import json
import random
import threading
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

TOKEN = "mock-token"
NOW = datetime.now(timezone.utc)


def ts(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ") if dt else None


RRULES = {
    "hourly": ("FREQ=HOURLY;INTERVAL=4", 4 / 24),
    "daily": ("FREQ=DAILY;INTERVAL=1", 1),
    "weekdays": ("FREQ=WEEKLY;INTERVAL=1;BYDAY=MO,TU,WE,TH,FR", 1),
    "weekly": ("FREQ=WEEKLY;INTERVAL=1;BYDAY=SU", 7),
    "fortnight": ("FREQ=WEEKLY;INTERVAL=2;BYDAY=SA", 14),
    "monthly": ("FREQ=MONTHLY;INTERVAL=1;BYMONTHDAY=1", 30.44),
    "once": ("FREQ=DAILY;INTERVAL=1;COUNT=1", None),
}


def build(version, seed):
    rnd = random.Random(seed)
    d = {"organizations": [], "users": [], "teams": [], "projects": [], "inventories": [], "credentials": [],
         "job_templates": [], "workflow_job_templates": [], "schedules": [], "notification_templates": [],
         "execution_environments": [], "jobs": [], "workflow_jobs": [], "hosts": 0, "instances": []}
    nid = [100]

    def new_id():
        nid[0] += 1
        return nid[0]

    migrated26 = version == "2.6"
    n_org = 30 if not migrated26 else 24          # 2.6 is missing 6 orgs so far
    orgs = ["Org-%02d" % i for i in range(1, n_org + 1)]
    for o in orgs:
        d["organizations"].append({"id": new_id(), "name": o, "summary_fields": {}})
    d["organizations"].insert(0, {"id": 1, "name": "Default", "summary_fields": {}})

    n_users = 300 if not migrated26 else 190
    for i in range(1, n_users + 1):
        last = NOW - timedelta(days=rnd.choice([0, 1, 2, 5, 12, 25, 40, 90, 200])) if (
            not migrated26 or rnd.random() < 0.55) else None
        d["users"].append({"id": new_id(), "username": "user%03d" % i, "last_login": ts(last),
                           "is_superuser": i <= 4})
    for o in orgs:
        for t in ("platform", "app"):
            d["teams"].append({"id": new_id(), "name": "%s-%s" % (o, t), "summary_fields": {"organization": {"name": o}}})
        for p in range(1, 5 if not migrated26 else 4):
            d["projects"].append({"id": new_id(), "name": "%s project %d" % (o, p), "scm_type": "git",
                                  "status": rnd.choice(["successful"] * 12 + ["failed"]),
                                  "summary_fields": {"organization": {"name": o}}})
        d["inventories"].append({"id": new_id(), "name": "%s inventory" % o, "summary_fields": {"organization": {"name": o}}})
        d["credentials"].append({"id": new_id(), "name": "%s ssh" % o, "summary_fields": {
            "organization": {"name": o}, "credential_type": {"name": "Machine"}}})
    d["credentials"].append({"id": new_id(), "name": "Built-in galaxy", "managed": True, "summary_fields": {"credential_type": {"name": "Galaxy"}}})
    d["execution_environments"].append({"id": new_id(), "name": "Default execution environment", "summary_fields": {}})
    d["execution_environments"].append({"id": new_id(), "name": "ee-custom-network", "summary_fields": {}})
    d["notification_templates"].append({"id": new_id(), "name": "Slack ops", "summary_fields": {"organization": {"name": "Org-01"}}})

    # templates: same deterministic names in both versions; 2.6 only has a subset
    shapes = ["hourly", "daily", "daily", "weekdays", "weekly", "weekly", "fortnight", "monthly", "monthly", "once", None, None, None]
    spec = random.Random(7)
    for oi, o in enumerate(orgs):
        for t in range(1, 9):
            name = "%s job %d" % (o, t)
            shape = spec.choice(shapes)
            wf = t == 8
            # migrated? ~60% of templates, and only in orgs present in 2.6
            present = (not migrated26) or (spec.random() < 0.6 and oi < n_org)
            if not present:
                continue
            kind_list = "workflow_job_templates" if wf else "job_templates"
            tid = new_id()
            d[kind_list].append({"id": tid, "name": name, "summary_fields": {"organization": {"name": o}}})
            # schedule state differs between platforms
            if shape:
                enabled = True
                if migrated26:
                    enabled = spec.choice([True, False, False])
                rr, period = RRULES[shape]
                nxt = None if shape == "once" else ts(NOW + timedelta(hours=5))
                d["schedules"].append({
                    "id": new_id(), "name": "%s schedule" % name, "enabled": enabled,
                    "rrule": "DTSTART:20260101T080000Z RRULE:" + rr, "next_run": nxt if enabled else None,
                    "unified_job_template": tid,
                    "summary_fields": {"unified_job_template": {
                        "id": tid, "name": name, "unified_job_type": "workflow_job" if wf else "job"}}})
            else:
                period, enabled = None, False
            # activity: runs for 60 days
            active = (not migrated26 and (shape is None or rnd.random() < 0.85)) or (
                migrated26 and rnd.random() < 0.5)
            runs = []
            if shape and period and active and enabled:
                t0 = NOW - timedelta(days=rnd.random() * 1)
                while t0 > NOW - timedelta(days=60):
                    runs.append(t0)
                    t0 -= timedelta(days=period)
            elif shape is None and active and rnd.random() < 0.7:
                for _ in range(rnd.randint(1, 6)):
                    runs.append(NOW - timedelta(days=rnd.random() * 55))
            for r in runs:
                st = rnd.choices(["successful", "failed", "error", "canceled"], [90, 7, 1, 2])[0]
                lt = "scheduled" if shape else "manual"
                sf = {"organization": {"name": o}, ("workflow_job_template" if wf else "job_template"): {"id": tid, "name": name}}
                if lt == "manual":
                    sf["created_by"] = {"username": "user%03d" % rnd.randint(1, 60)}
                rec = {"id": new_id(), "name": name, "created": ts(r), "status": st, "launch_type": lt,
                       "elapsed": round(rnd.uniform(5, 900), 1), "summary_fields": sf}
                (d["workflow_jobs"] if wf else d["jobs"]).append(rec)
    d["jobs"].sort(key=lambda j: j["created"], reverse=True)
    d["workflow_jobs"].sort(key=lambda j: j["created"], reverse=True)
    d["hosts"] = 1800 if not migrated26 else 900
    for i in range(3 if not migrated26 else 4):
        d["instances"].append({"hostname": "node%d" % i, "node_type": "execution" if i else "hybrid", "capacity": 100,
                               "consumed_capacity": rnd.randint(5, 80), "enabled": True,
                               "errors": "" if i != 2 or migrated26 else "unreachable"})
    # a few extra things
    d["jobs_running"] = rnd.randint(2, 9) if not migrated26 else rnd.randint(0, 3)
    return d


def make_handler(data, prefix, gw_prefix, version):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def send(self, code, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            u = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            path = u.path
            if path == prefix + "/ping/":
                return self.send(200, {"ha": False, "version": "4.5.9" if version == "2.4" else "4.7.2", "active_node": "node0"})
            if self.headers.get("Authorization") != "Bearer " + TOKEN:
                return self.send(401, {"detail": "Authentication credentials were not provided."})
            if path == prefix + "/me/":
                return self.send(200, {"count": 1, "results": [{"username": "admin"}]})
            routes = {"/jobs/": "jobs", "/workflow_jobs/": "workflow_jobs", "/instances/": "instances"}
            key = None
            for suffix, name in routes.items():
                if path == prefix + suffix:
                    key = name
            if key is None:
                for cat in ("organizations", "users", "teams", "projects", "inventories", "credentials", "job_templates",
                            "workflow_job_templates", "schedules", "notification_templates", "execution_environments"):
                    if path == prefix + "/%s/" % cat or (gw_prefix and path == gw_prefix + "/%s/" % cat):
                        key = cat
            if path == prefix + "/hosts/":
                return self.send(200, {"count": data["hosts"], "results": []})
            if key is None:
                return self.send(404, {"detail": "Not found."})
            items = data[key]
            if key == "jobs" and q.get("status") in ("running", "pending", "waiting"):
                n = data["jobs_running"] if q["status"] == "running" else 0
                return self.send(200, {"count": n, "results": []})
            if key == "workflow_jobs" and q.get("status") in ("running", "pending", "waiting"):
                return self.send(200, {"count": 0, "results": []})
            if "created__gte" in q:
                items = [i for i in items if i["created"] >= q["created__gte"].replace("Z", ".000000Z")]
            size, page = min(int(q.get("page_size", 25)), 200), int(q.get("page", 1))
            chunk = items[(page - 1) * size: page * size]
            nxt = None
            if page * size < len(items):
                qq = dict(q, page=page + 1)
                nxt = u.path + "?" + "&".join("%s=%s" % (k, v) for k, v in qq.items())
            return self.send(200, {"count": len(items), "next": nxt, "previous": None, "results": chunk})
    return H


def serve(port, handler):
    srv = ThreadingHTTPServer(("127.0.0.1", port), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port24", type=int, default=8424)
    ap.add_argument("--port26", type=int, default=8426)
    a = ap.parse_args()
    d24, d26 = build("2.4", 24), build("2.6", 26)
    serve(a.port24, make_handler(d24, "/api/v2", "", "2.4"))
    serve(a.port26, make_handler(d26, "/api/controller/v2", "/api/gateway/v1", "2.6"))
    print("Mock AAP 2.4 on http://127.0.0.1:%d  (jobs=%d)" % (a.port24, len(d24["jobs"])))
    print("Mock AAP 2.6 on http://127.0.0.1:%d  (jobs=%d)" % (a.port26, len(d26["jobs"])))
    print('Token: "%s".  Ctrl-C to stop.' % TOKEN)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
