#!/usr/bin/env python3
"""
aap_migration_dashboard.py - Daily AAP 2.4 vs AAP 2.6 migration dashboard.

Queries the REST APIs of an Ansible Automation Platform 2.4 controller and an
AAP 2.6 platform (via the platform gateway), compares them, and writes:

  <output-dir>/index.html                    self-contained dashboard (no CDN/JS deps)
  <output-dir>/migration_gap_latest.csv      everything still to migrate / act on
  <output-dir>/history.json                  one entry per day, drives the trend chart
  <output-dir>/snapshots/snapshot_<date>.json   full daily snapshot (no secrets)
  <output-dir>/snapshots/migration_gap_<date>.csv
  <output-dir>/snapshots/dashboard_<date>.html  archived copy of each day's dashboard

What it measures (per platform, then compared)
  * Jobs run in the last 24h, jobs running / pending right now
  * Active (enabled) schedules
  * Active users (logged in within --active-days) and total users
  * Organizations, projects (plus teams, inventories, credentials, templates, ...)
  * Unique jobs: a job template / workflow template counts ONCE no matter how many
    times it ran. Split by cadence (daily / weekly / monthly / ...) two ways:
      - scheduled cadence, parsed from each enabled schedule's RRULE
      - observed cadence, from the median gap between runs in the history window
  * Migration parity: objects present in both / only in 2.4 (pending) / only in 2.6
  * Cutover status of every unique job (not migrated, awaiting cutover,
    dual-scheduled duplicate-run risk, cutover complete, dormant)
  * User adoption: 2.4-active users who now exist / have logged in on 2.6
  * Failure rate, launch types, durations, top templates, capacity, project health

Requirements: Python 3.8+, standard library only.

Authentication (environment variables - never put secrets on the command line):
  AAP24_HOST   AAP24_TOKEN   (or AAP24_USERNAME + AAP24_PASSWORD)
  AAP26_HOST   AAP26_TOKEN   (or AAP26_USERNAME + AAP26_PASSWORD)
  The token needs read access across all organizations (system auditor or
  superuser). Hosts may be given with or without https://.

Usage
  python3 aap_migration_dashboard.py --output-dir /var/www/html/aap-migration
  python3 aap_migration_dashboard.py --serve 8080          # view at http://127.0.0.1:8080
  # cron (daily 06:00):
  # 0 6 * * * . /etc/aap-dashboard.env && /usr/bin/python3 /opt/aap/aap_migration_dashboard.py -o /var/www/html/aap-migration -q

Exit codes: 0 = all queries succeeded, 1 = partial (some queries or one platform
failed - dashboard still written), 2 = neither platform usable / bad arguments.

All times are UTC. Object matching between platforms is by (organization, name)
with case/whitespace normalised, because numeric IDs differ between installs.
"""
import argparse
import csv
import html
import json
import os
import re
import ssl
import statistics
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import error, parse, request

VERSION = "1.0.0"

# --------------------------------------------------------------------------- #
# Configuration constants
# --------------------------------------------------------------------------- #

# Built-in / sample objects that differ between versions and should not count as
# "pending migration". Matched case-insensitively against the object name.
DEFAULT_EXCLUDES = [
    r"^demo (job template|project|inventory|credential)$",
    r"^default$",
    r"^(control plane|default|minimal) execution environment$",
    r"^cleanup (job|activity) schedule$",
    r"^cleanup expired (sessions|oauth 2 tokens)$",
    r"^cleanup (job details|activity stream)$",
]

CATEGORIES = [
    ("organizations", "Organizations"),
    ("users", "Users"),
    ("teams", "Teams"),
    ("projects", "Projects"),
    ("inventories", "Inventories"),
    ("credentials", "Credentials"),
    ("job_templates", "Job templates"),
    ("workflow_job_templates", "Workflow templates"),
    ("schedules", "Schedules"),
    ("notification_templates", "Notification templates"),
    ("execution_environments", "Execution environments"),
]
LIST_PATHS = {
    "organizations": "/organizations/",
    "users": "/users/",
    "teams": "/teams/",
    "projects": "/projects/",
    "inventories": "/inventories/",
    "credentials": "/credentials/",
    "job_templates": "/job_templates/",
    "workflow_job_templates": "/workflow_job_templates/",
    "schedules": "/schedules/",
    "notification_templates": "/notification_templates/",
    "execution_environments": "/execution_environments/",
}

BUCKETS = [
    ("sub_daily", "Sub-daily (hourly, etc.)"),
    ("daily", "Daily"),
    ("multi_weekly", "Several times a week"),
    ("weekly", "Weekly"),
    ("fortnightly", "Every 2-3 weeks"),
    ("monthly", "Monthly"),
    ("rare", "Quarterly or less"),
    ("one_time", "One-time"),
    ("single_run", "Single run in window"),
    ("unknown", "Unparsed / unknown"),
]
BUCKET_LABEL = dict(BUCKETS)

# key -> (label, css class, explanation)
JOB_STATUS_META = {
    "not_migrated_active": ("Not migrated - active in 2.4", "bad",
                            "Still scheduled or running in 2.4 and missing from 2.6. Highest priority."),
    "dual_scheduled": ("Dual-scheduled (duplicate-run risk)", "bad",
                       "Enabled schedules exist on BOTH platforms - the automation may run twice."),
    "awaiting_cutover": ("Migrated - awaiting cutover", "warn",
                         "Exists in 2.6 but is not yet scheduled/running there while 2.4 is still active."),
    "dual_manual": ("In use on both", "warn",
                    "Runs recently on both platforms but not scheduled on both."),
    "cutover_complete": ("Cutover complete", "good",
                         "Active in 2.6 and no longer active in 2.4."),
    "dormant": ("Migrated - dormant", "neutral",
                "Exists in both, neither has a schedule or recent runs."),
    "not_migrated_inactive": ("Not migrated - inactive in 2.4", "neutral",
                              "No schedule or runs in the history window. Candidate to retire instead of migrate."),
}
JOB_STATUS_ORDER = list(JOB_STATUS_META)
KIND_LABEL = {"jt": "Job template", "wfjt": "Workflow template"}


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #

def norm(value):
    """Normalise a name for cross-platform matching."""
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()


def parse_dt(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = datetime.strptime(str(value)[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None


def pct(part, whole):
    if not whole:
        return None
    return round(part * 100.0 / whole, 1)


def percentile(values, q):
    if not values:
        return None
    ordered = sorted(values)
    idx = int(round(q * (len(ordered) - 1)))
    return ordered[max(0, min(len(ordered) - 1, idx))]


def log(msg, quiet=False):
    if not quiet:
        print(msg, file=sys.stderr)


# --------------------------------------------------------------------------- #
# Schedule cadence (RRULE) and run-gap cadence
# --------------------------------------------------------------------------- #

def bucket_for_period(days):
    """Map an average period in days to a cadence bucket key."""
    if days is None:
        return "unknown"
    if days < 0.8:
        return "sub_daily"
    if days <= 1.6:
        return "daily"
    if days < 5.5:
        return "multi_weekly"
    if days <= 10:
        return "weekly"
    if days < 25:
        return "fortnightly"
    if days <= 45:
        return "monthly"
    return "rare"


def _int(value, default=1):
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return default


def rrule_period(rrule):
    """
    Return (period_days, one_time) for an AAP schedule rrule string.

    AAP stores e.g. "DTSTART:20260101T080000Z RRULE:INTERVAL=1;FREQ=DAILY".
    period_days is the average days between runs (the most frequent RRULE wins
    when several are combined); None if nothing could be parsed. A rule with
    COUNT=1 is how AAP represents "run once".
    """
    periods, one_time = [], False
    for rule in re.findall(r"RRULE:([^\s]+)", rrule or "", flags=re.I):
        params = {}
        for part in rule.split(";"):
            if "=" in part:
                k, v = part.split("=", 1)
                params[k.strip().upper()] = v.strip()
        freq = params.get("FREQ", "").upper()
        interval = _int(params.get("INTERVAL"))
        if params.get("COUNT") == "1":
            one_time = True
            continue
        if freq == "MINUTELY":
            periods.append(interval / 1440.0)
        elif freq == "HOURLY":
            periods.append(interval / 24.0)
        elif freq == "DAILY":
            periods.append(float(interval))
        elif freq == "WEEKLY":
            ndays = len([d for d in params.get("BYDAY", "").split(",") if d]) or 1
            periods.append(7.0 * interval / ndays)
        elif freq == "MONTHLY":
            per_month = (len([d for d in params.get("BYMONTHDAY", "").split(",") if d])
                         or len([d for d in params.get("BYSETPOS", "").split(",") if d]) or 1)
            periods.append(30.44 * interval / per_month)
        elif freq == "YEARLY":
            periods.append(365.0 * interval)
    if periods:
        return min(periods), False
    return None, one_time


def cadence_from_rrule(rrule):
    period, one_time = rrule_period(rrule)
    if one_time and period is None:
        return None, "one_time"
    return period, bucket_for_period(period)


# --------------------------------------------------------------------------- #
# API client (stdlib only)
# --------------------------------------------------------------------------- #

class ApiError(Exception):
    pass


class Client:
    def __init__(self, label, host, ctl_prefix, gw_prefix, token, username, password,
                 verify=True, ca_bundle=None, timeout=60, retries=3):
        host = (host or "").strip().rstrip("/")
        if host and not re.match(r"^https?://", host, flags=re.I):
            host = "https://" + host
        self.label, self.base = label, host
        self.ctl_prefix = "/" + ctl_prefix.strip("/")
        self.gw_prefix = ("/" + gw_prefix.strip("/")) if gw_prefix else ""
        self.token, self.username, self.password = token, username, password
        self.timeout, self.retries = timeout, retries
        if verify:
            self.ctx = ssl.create_default_context(cafile=ca_bundle) if ca_bundle else ssl.create_default_context()
        else:
            self.ctx = ssl.create_default_context()
            self.ctx.check_hostname = False
            self.ctx.verify_mode = ssl.CERT_NONE
        self.truncated = set()
        self.request_count = 0

    def ctl(self, path):
        return self.ctl_prefix + path

    def gw(self, path):
        return self.gw_prefix + path

    def _headers(self):
        headers = {"Accept": "application/json", "User-Agent": "aap-migration-dashboard/" + VERSION}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        elif self.username:
            import base64
            raw = ("%s:%s" % (self.username, self.password or "")).encode("utf-8")
            headers["Authorization"] = "Basic " + base64.b64encode(raw).decode("ascii")
        return headers

    def get_json(self, path_or_url):
        url = path_or_url if re.match(r"^https?://", path_or_url) else self.base + path_or_url
        last = None
        for attempt in range(self.retries + 1):
            try:
                self.request_count += 1
                req = request.Request(url, headers=self._headers())
                with request.urlopen(req, timeout=self.timeout, context=self.ctx) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except error.HTTPError as exc:
                if exc.code in (401, 403):
                    raise ApiError("HTTP %d from %s - check the token and its permissions" % (exc.code, url))
                if exc.code == 404:
                    raise ApiError("HTTP 404 for %s - check the API prefix for this platform" % url)
                last = "HTTP %d" % exc.code
            except (error.URLError, OSError, ValueError) as exc:
                reason = getattr(exc, "reason", exc)
                last = str(reason)
            time.sleep(min(2 ** attempt, 8))
        raise ApiError("%s (%s after %d attempts)" % (url, last, self.retries + 1))

    def count(self, path, params=None):
        q = dict(params or {})
        q["page_size"] = 1
        return int(self.get_json(path + "?" + parse.urlencode(q)).get("count", 0))

    def iter_all(self, path, params=None, page_size=200, max_items=None):
        q = dict(params or {})
        q["page_size"] = page_size
        url, seen = path + "?" + parse.urlencode(q), 0
        while url:
            data = self.get_json(url)
            for item in data.get("results", []):
                yield item
                seen += 1
                if max_items and seen >= max_items:
                    self.truncated.add(path)
                    return
            url = data.get("next") or None


# --------------------------------------------------------------------------- #
# Collection - one platform
# --------------------------------------------------------------------------- #

def summ(obj, name):
    return (obj.get("summary_fields") or {}).get(name) or {}


def org_of(obj):
    return summ(obj, "organization").get("name") or ""


def get_job_rec(index, kind, org, name, exists=False, obj_id=None):
    key = (kind, norm(org), norm(name))
    rec = index.get(key)
    if rec is None:
        rec = {"kind": kind, "org": org or "", "name": name or "?", "exists": exists, "id": obj_id,
               "sched_enabled": 0, "sched_total": 0, "sched_period": None,
               "runs_window": 0, "runs_24h": 0, "last_run": None, "_times": [],
               "obs_bucket": None, "median_gap_days": None}
        index[key] = rec
    elif exists:
        rec["exists"], rec["id"] = True, obj_id
    return rec


def collect_side(c, args, now):
    """Collect everything for one platform. Never raises; problems land in errors."""
    s = {"label": c.label, "host": c.base, "reachable": False, "authenticated": False,
         "version": None, "errors": [], "notes": [], "_raw": {}, "_jobs": {}}

    def safe(name, fn, default=None):
        try:
            return fn()
        except (ApiError, ValueError, KeyError, TypeError) as exc:
            s["errors"].append("%s: %s" % (name, exc))
            return default

    try:
        ping = c.get_json(c.ctl("/ping/"))
    except ApiError as exc:
        s["errors"].append("Cannot reach the API: %s" % exc)
        return s
    s.update(reachable=True, version=ping.get("version"), ha=ping.get("ha"),
             active_node=ping.get("active_node"))
    try:
        c.get_json(c.ctl("/me/"))
        s["authenticated"] = True
    except ApiError as exc:
        s["errors"].append("Authentication failed: %s" % exc)
        return s

    # ---- configuration objects (full lists, needed for name matching) ----
    raw = s["_raw"]
    for cat, path in LIST_PATHS.items():
        raw[cat] = safe(cat, lambda p=path: list(c.iter_all(c.ctl(p), max_items=args.max_objects)))
    if c.truncated:
        s["notes"].append("Object lists were capped at --max-objects=%d." % args.max_objects)

    # Users and organizations: the 2.6 gateway is the source of truth for logins.
    s["users_source"] = "controller"
    if c.gw_prefix:
        try:
            gw_users = list(c.iter_all(c.gw("/users/"), max_items=args.max_objects))
            ctl_users = c.count(c.ctl("/users/"))
            raw["users"], s["users_source"] = gw_users, "gateway"
            if ctl_users != len(gw_users):
                s["notes"].append("Gateway has %d users but the controller has %d - user sync between "
                                  "gateway and controller may be incomplete." % (len(gw_users), ctl_users))
            gw_orgs = c.count(c.gw("/organizations/"))
            ctl_orgs = len(raw.get("organizations") or [])
            if raw.get("organizations") is not None and gw_orgs != ctl_orgs:
                s["notes"].append("Gateway has %d organizations but the controller has %d." % (gw_orgs, ctl_orgs))
        except ApiError as exc:
            s["notes"].append("Gateway user/org lookup unavailable (%s); used controller data. "
                              "Controller last_login may be stale on 2.5+ because logins happen at the gateway." % exc)

    # ---- live counts ----
    live = {}
    for name, path, params in [
        ("running", "/jobs/", {"status": "running"}),
        ("pending", "/jobs/", {"status": "pending"}),
        ("waiting", "/jobs/", {"status": "waiting"}),
        ("workflow_running", "/workflow_jobs/", {"status": "running"}),
        ("hosts", "/hosts/", {}),
    ]:
        live[name] = safe("count " + name, lambda p=path, q=params: c.count(c.ctl(p), q))
    s["live"] = live

    # ---- capacity ----
    insts = safe("instances", lambda: list(c.iter_all(c.ctl("/instances/"))), [])
    if insts:
        cap = sum(float(i.get("capacity") or 0) for i in insts)
        used = sum(float(i.get("consumed_capacity") or 0) for i in insts)
        s["capacity"] = {
            "instances": len(insts),
            "capacity": round(cap, 1), "consumed": round(used, 1), "used_pct": pct(used, cap),
            "with_errors": sum(1 for i in insts if i.get("errors")),
            "disabled": sum(1 for i in insts if i.get("enabled") is False),
            "node_types": dict(Counter(i.get("node_type", "?") for i in insts)),
        }

    # ---- unique-job index from templates ----
    index, id_map = {}, {}
    for kind, cat in (("jt", "job_templates"), ("wfjt", "workflow_job_templates")):
        for o in raw.get(cat) or []:
            rec = get_job_rec(index, kind, org_of(o), o.get("name"), True, o.get("id"))
            id_map[o.get("id")] = (kind, rec)

    # ---- schedules ----
    sched = {"total": 0, "enabled": 0, "disabled": 0, "enabled_jobs": 0, "enabled_other": 0,
             "dead": 0, "buckets_all": Counter(), "buckets_unique": Counter()}
    sched_rows = []
    for sc in raw.get("schedules") or []:
        ujt = summ(sc, "unified_job_template")
        ujt_id = ujt.get("id") or (sc.get("unified_job_template")
                                   if isinstance(sc.get("unified_job_template"), int) else None)
        utype = (ujt.get("unified_job_type") or "").lower()
        if ujt_id in id_map:
            kind = id_map[ujt_id][0]
        elif utype in ("job", "job_template"):
            kind = "jt"
        elif utype in ("workflow_job", "workflow_job_template"):
            kind = "wfjt"
        else:
            kind = "other"
        enabled = bool(sc.get("enabled"))
        period, bucket = cadence_from_rrule(sc.get("rrule"))
        sched["total"] += 1
        sched["enabled" if enabled else "disabled"] += 1
        parent = id_map[ujt_id][1] if ujt_id in id_map else None
        sched_rows.append({"name": sc.get("name"), "kind": kind, "enabled": enabled,
                           "parent_name": ujt.get("name"), "parent_org": parent["org"] if parent else ""})
        if enabled:
            if kind in ("jt", "wfjt"):
                sched["enabled_jobs"] += 1
                sched["buckets_all"][bucket] += 1
            else:
                sched["enabled_other"] += 1
            if not sc.get("next_run") and bucket != "one_time":
                sched["dead"] += 1
        if kind in ("jt", "wfjt"):
            if ujt_id in id_map:
                rec = id_map[ujt_id][1]
            else:
                rec = get_job_rec(index, kind, "", ujt.get("name"), False)
            rec["sched_total"] += 1
            if enabled:
                rec["sched_enabled"] += 1
                if period is not None and (rec["sched_period"] is None or period < rec["sched_period"]):
                    rec["sched_period"] = period
                rec.setdefault("sched_buckets", []).append(bucket)
    for rec in index.values():
        if rec["sched_enabled"]:
            b = bucket_for_period(rec["sched_period"]) if rec["sched_period"] is not None else (
                "one_time" if "one_time" in rec.get("sched_buckets", []) else "unknown")
            rec["sched_bucket"] = b
            sched["buckets_unique"][b] += 1
    s["schedules"] = sched
    s["_sched_rows"] = sched_rows

    # ---- job history ----
    since = now - timedelta(days=args.history_days)
    day24 = now - timedelta(hours=24)
    act = {"jobs_24h": 0, "workflow_24h": 0, "jobs_window": 0, "workflow_window": 0,
           "status_24h": Counter(), "launch_24h": Counter(), "status_window": Counter(),
           "elapsed": [], "launchers": set(), "daily": {}, "history_ok": False}
    days = [(now - timedelta(days=i)).date() for i in range(13, -1, -1)]
    for d in days:
        act["daily"][d.isoformat()] = [0, 0]
    for kind, path, tag in (("jt", "/jobs/", "jobs"), ("wfjt", "/workflow_jobs/", "workflow")):
        jobs = safe("job history " + path, lambda p=path: c.iter_all(
            c.ctl(p), {"created__gte": iso(since), "order_by": "-created"}, max_items=args.max_jobs))
        if jobs is None:
            continue
        try:
            for j in jobs:
                created = parse_dt(j.get("created"))
                if not created:
                    continue
                sf = j.get("summary_fields") or {}
                tmpl = sf.get("job_template") or sf.get("workflow_job_template") or sf.get("unified_job_template") or {}
                tid = tmpl.get("id") or j.get("job_template") or j.get("workflow_job_template") or j.get("unified_job_template")
                if tid in id_map:
                    rec = id_map[tid][1]
                else:
                    rec = get_job_rec(index, kind, (sf.get("organization") or {}).get("name") or "",
                                      tmpl.get("name") or j.get("name"), False)
                rec["runs_window"] += 1
                rec["_times"].append(created)
                if rec["last_run"] is None or created > rec["last_run"]:
                    rec["last_run"] = created
                act[tag + "_window"] += 1
                status = j.get("status") or "unknown"
                act["status_window"][status] += 1
                if tag == "jobs" and j.get("elapsed") and status in ("successful", "failed", "error"):
                    act["elapsed"].append(float(j["elapsed"]))
                who = (sf.get("created_by") or {}).get("username")
                if who:
                    act["launchers"].add(who)
                dkey = created.date().isoformat()
                if dkey in act["daily"]:
                    act["daily"][dkey][0 if tag == "jobs" else 1] += 1
                if created >= day24:
                    rec["runs_24h"] += 1
                    act[tag + "_24h"] += 1
                    if tag == "jobs":
                        act["status_24h"][status] += 1
                        act["launch_24h"][j.get("launch_type") or "unknown"] += 1
            act["history_ok"] = True
        except ApiError as exc:
            s["errors"].append("job history %s: %s" % (path, exc))
        if path in c.truncated:
            s["notes"].append("%s history was capped at --max-jobs=%d (newest first); older days are "
                              "incomplete, 24h figures are unaffected." % (path, args.max_jobs))

    # observed cadence per unique job
    for rec in index.values():
        times = sorted(rec["_times"])
        if len(times) >= 2:
            gaps = [(b - a).total_seconds() / 86400.0 for a, b in zip(times, times[1:])]
            rec["median_gap_days"] = statistics.median(gaps)
            rec["obs_bucket"] = bucket_for_period(rec["median_gap_days"])
        elif len(times) == 1:
            rec["obs_bucket"] = "single_run"
        rec["active"] = bool(rec["sched_enabled"] or rec["runs_window"])
    s["_jobs"] = index

    obs = Counter(r["obs_bucket"] for r in index.values() if r["obs_bucket"])
    top = sorted((r for r in index.values() if r["runs_window"]), key=lambda r: -r["runs_window"])[:10]
    fails = act["status_24h"].get("failed", 0) + act["status_24h"].get("error", 0)
    total24 = sum(act["status_24h"].values())
    act.update(
        launchers=len(act["launchers"]),
        failure_rate_24h=pct(fails, total24),
        elapsed_avg=round(statistics.mean(act["elapsed"]), 1) if act["elapsed"] else None,
        elapsed_p95=round(percentile(act["elapsed"], 0.95), 1) if act["elapsed"] else None,
        top_templates=[{"kind": r["kind"], "org": r["org"], "name": r["name"], "runs": r["runs_window"]} for r in top],
        unique_ran_window=sum(1 for r in index.values() if r["runs_window"]),
        unique_ran_24h=sum(1 for r in index.values() if r["runs_24h"]),
        observed_buckets=dict(obs),
    )
    del act["elapsed"]
    act["status_24h"], act["launch_24h"] = dict(act["status_24h"]), dict(act["launch_24h"])
    act["status_window"] = dict(act["status_window"])
    s["activity"] = act

    # ---- users / projects / counts ----
    users = raw.get("users")
    if users is not None:
        cutoff = now - timedelta(days=args.active_days)
        logins = [parse_dt(u.get("last_login")) for u in users]
        s["users"] = {
            "total": len(users),
            "active": sum(1 for d in logins if d and d >= cutoff),
            "never_logged_in": sum(1 for d in logins if d is None),
            "superusers": sum(1 for u in users if u.get("is_superuser")),
            "source": s["users_source"],
        }
    projects = raw.get("projects")
    if projects is not None:
        st = Counter((p.get("status") or "unknown") for p in projects)
        s["project_status"] = dict(st)
    s["counts"] = {cat: (len(v) if v is not None else None) for cat, v in raw.items()}
    s["counts"]["hosts"] = live.get("hosts")
    s["counts"]["unique_defined"] = sum(1 for r in index.values() if r["exists"])
    s["counts"]["unique_scheduled"] = sum(1 for r in index.values() if r["exists"] and r["sched_enabled"])
    s["request_count"] = c.request_count
    return s


# --------------------------------------------------------------------------- #
# Comparison
# --------------------------------------------------------------------------- #

def build_records(s, cat, rename_org):
    """Return {normalised_key: record} for one category, or None if unavailable."""
    raw = (s.get("_raw") or {}).get(cat)
    if raw is None:
        return None
    out = {}
    for o in raw:
        if o.get("managed"):
            continue
        sf = o.get("summary_fields") or {}
        org = rename_org(org_of(o))
        name = o.get("name") or ""
        detail, parts = "", None
        if cat == "organizations":
            name, org = rename_org(name), ""
            parts = (name,)
        elif cat == "users":
            name, org = o.get("username") or "", ""
            last = parse_dt(o.get("last_login"))
            detail = "last login " + last.strftime("%Y-%m-%d") if last else "never logged in"
            parts = (name,)
        elif cat == "credentials":
            ctype = (sf.get("credential_type") or {}).get("name") or ""
            detail = ctype
            parts = (org, ctype, name)
        elif cat == "projects":
            detail = "%s / %s" % (o.get("scm_type") or "manual", o.get("status") or "?")
            parts = (org, name)
        elif cat == "schedules":
            continue  # handled below from pre-resolved rows
        else:
            parts = (org, name)
        out[tuple(norm(p) for p in parts)] = {"org": org, "name": name, "detail": detail,
                                              "last_login": parse_dt(o.get("last_login")) if cat == "users" else None}
    if cat == "schedules":
        for r in s.get("_sched_rows") or []:
            org = rename_org(r["parent_org"])
            key = (r["kind"], norm(org), norm(r["parent_name"]), norm(r["name"]))
            out[key] = {"org": org, "name": "%s / %s" % (r["parent_name"], r["name"]),
                        "detail": ("enabled" if r["enabled"] else "disabled"), "_raw_name": r["name"]}
    return out


def compare_sides(a, b, args, now):
    excludes = [re.compile(p, re.I) for p in (([] if args.no_default_excludes else DEFAULT_EXCLUDES)
                                              + (args.exclude_regex or []))]
    renames = {}
    for pair in args.org_rename or []:
        if "=" in pair:
            old, new = pair.split("=", 1)
            renames[norm(old)] = new.strip()

    def rename_org(org):
        return renames.get(norm(org), org)

    def excluded(rec):
        nm = rec.get("_raw_name") or rec["name"]
        return any(p.search(nm) for p in excludes)

    ident = lambda org: org  # 2.6 side is never renamed
    res = {"categories": [], "pending_rows": [], "jobs": {}, "adoption": {}, "usable": False}
    for cat, label in CATEGORIES:
        ra = build_records(a, cat, rename_org) if a.get("authenticated") else None
        rb = build_records(b, cat, ident) if b.get("authenticated") else None
        entry = {"key": cat, "label": label, "available": ra is not None and rb is not None}
        if not entry["available"]:
            entry.update(n24=len(ra) if ra is not None else None, n26=len(rb) if rb is not None else None,
                         both=None, pending=None, new26=None, pct=None)
            res["categories"].append(entry)
            continue
        ra = {k: v for k, v in ra.items() if not excluded(v)}
        rb = {k: v for k, v in rb.items() if not excluded(v)}
        both = set(ra) & set(rb)
        pend = sorted(set(ra) - set(rb), key=lambda k: (ra[k]["org"].casefold(), ra[k]["name"].casefold()))
        new = set(rb) - set(ra)
        entry.update(n24=len(ra), n26=len(rb), both=len(both), pending=len(pend), new26=len(new),
                     pct=pct(len(both), len(ra)))
        res["categories"].append(entry)
        for k in pend:
            res["pending_rows"].append({"category": label, "org": ra[k]["org"], "name": ra[k]["name"],
                                        "status": "Not in 2.6", "detail": ra[k]["detail"]})
        if cat == "users":
            res["_users"] = (ra, rb)
    avail = [c for c in res["categories"] if c["available"] and c["n24"]]
    tot24 = sum(c["n24"] for c in avail)
    tot_both = sum(c["both"] for c in avail)
    res["overall_pct"] = pct(tot_both, tot24)
    res["usable"] = bool(avail)

    # ---- unique-job cutover status ----
    ja, jb = a.get("_jobs") or {}, b.get("_jobs") or {}
    if a.get("authenticated") and b.get("authenticated"):
        b_by_key = {(r["kind"], norm(r["org"]), norm(r["name"])): r for r in jb.values() if r["exists"]}
        a_keys = set()
        rows, counts = [], Counter()
        for r in ja.values():
            if not r["exists"] or any(p.search(r["name"]) for p in excludes):
                continue
            org = rename_org(r["org"])
            k = (r["kind"], norm(org), norm(r["name"]))
            a_keys.add(k)
            rb_ = b_by_key.get(k)
            act24 = bool(r["sched_enabled"] or r["runs_window"])
            if rb_ is None:
                status = "not_migrated_active" if act24 else "not_migrated_inactive"
            else:
                act26 = bool(rb_["sched_enabled"] or rb_["runs_window"])
                if r["sched_enabled"] and rb_["sched_enabled"]:
                    status = "dual_scheduled"
                elif act24 and act26:
                    status = "dual_manual"
                elif act24:
                    status = "awaiting_cutover"
                elif act26:
                    status = "cutover_complete"
                else:
                    status = "dormant"
            counts[status] += 1
            rows.append({
                "kind": KIND_LABEL[r["kind"]], "org": org, "name": r["name"], "status": status,
                "sched24": r["sched_enabled"], "sched26": rb_["sched_enabled"] if rb_ else None,
                "runs24": r["runs_window"], "runs26": rb_["runs_window"] if rb_ else None,
                "last24": iso(r["last_run"]), "last26": iso(rb_["last_run"]) if rb_ and rb_["last_run"] else None,
            })
        order = {k: i for i, k in enumerate(JOB_STATUS_ORDER)}
        rows.sort(key=lambda x: (order[x["status"]], x["org"].casefold(), x["name"].casefold()))
        new26 = sum(1 for k, r in b_by_key.items() if k not in a_keys
                    and not any(p.search(r["name"]) for p in excludes))
        migrated = sum(v for k, v in counts.items() if not k.startswith("not_migrated"))
        total = sum(counts.values())
        res["jobs"] = {"rows": rows, "counts": dict(counts), "total24": total, "migrated": migrated,
                       "migrated_pct": pct(migrated, total), "new26": new26}
        for r in rows:
            if r["status"] in ("not_migrated_active", "dual_scheduled", "awaiting_cutover", "dual_manual"):
                res["pending_rows"].append({
                    "category": "Unique job - " + r["kind"], "org": r["org"], "name": r["name"],
                    "status": JOB_STATUS_META[r["status"]][0],
                    "detail": "2.4 enabled schedules=%s, 2.6 enabled schedules=%s, runs in window 2.4=%s 2.6=%s" % (
                        r["sched24"], r["sched26"], r["runs24"], r["runs26"])})

    # ---- user adoption ----
    if "_users" in res:
        ua, ub = res.pop("_users")
        cutoff = now - timedelta(days=args.active_days)
        active24 = {k for k, v in ua.items() if v["last_login"] and v["last_login"] >= cutoff}
        exist26 = active24 & set(ub)
        logged26 = {k for k in exist26 if ub[k]["last_login"]}
        res["adoption"] = {
            "active24": len(active24), "exist26": len(exist26), "logged26": len(logged26),
            "missing26": len(active24 - set(ub)),
            "logged26_pct": pct(len(logged26), len(active24)),
            "exist26_pct": pct(len(exist26), len(active24)),
        }
        for k in sorted(active24 - set(ub)):
            res["pending_rows"].append({"category": "Active user not in 2.6", "org": "", "name": ua[k]["name"],
                                        "status": "Not in 2.6", "detail": ua[k]["detail"]})
    return res


# --------------------------------------------------------------------------- #
# Persistence: history, snapshot, CSV
# --------------------------------------------------------------------------- #

def strip_private(obj):
    if isinstance(obj, dict):
        return {k: strip_private(v) for k, v in obj.items() if not str(k).startswith("_")}
    if isinstance(obj, (list, tuple)):
        return [strip_private(v) for v in obj]
    if isinstance(obj, (set, frozenset)):
        return sorted(strip_private(v) for v in obj)
    if isinstance(obj, datetime):
        return iso(obj)
    return obj


def csv_safe(value):
    text = "" if value is None else str(value)
    return "'" + text if text[:1] in ("=", "+", "-", "@", "\t", "\r") else text


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["category", "organization", "name", "status", "detail"])
        for r in rows:
            w.writerow([csv_safe(r[k]) for k in ("category", "org", "name", "status", "detail")])


def update_history(out_dir, today, s24, s26, cmp_):
    path = out_dir / "history.json"
    try:
        history = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(history, list):
            history = []
    except (OSError, ValueError):
        history = []
    entry = {
        "date": today,
        "overall_pct": cmp_.get("overall_pct"),
        "categories": {c["key"]: c["pct"] for c in cmp_["categories"]},
        "jobs_24h": [(s24.get("activity") or {}).get("jobs_24h"), (s26.get("activity") or {}).get("jobs_24h")],
        "unique_status": (cmp_.get("jobs") or {}).get("counts", {}),
        "unique_migrated_pct": (cmp_.get("jobs") or {}).get("migrated_pct"),
        "users_active": [(s24.get("users") or {}).get("active"), (s26.get("users") or {}).get("active")],
    }
    history = [h for h in history if h.get("date") != today] + [entry]
    history = sorted(history, key=lambda h: h["date"])[-365:]
    path.write_text(json.dumps(history, indent=1), encoding="utf-8")
    return history


# --------------------------------------------------------------------------- #
# HTML rendering
# --------------------------------------------------------------------------- #

CSS = """
:root{--bg:#f6f7f9;--card:#fff;--ink:#1c2230;--muted:#5d6678;--line:#e1e5ec;--c24:#c4600c;--c26:#1b66c9;
--good:#17794a;--warn:#a96a00;--bad:#b83227;--neutral:#5d6678;--goodbg:#e4f4ea;--warnbg:#fdf0d5;--badbg:#fbe6e3;--neutralbg:#eceff3}
@media (prefers-color-scheme:dark){:root{--bg:#10141c;--card:#181e29;--ink:#e8ecf3;--muted:#9aa4b6;--line:#2a3243;
--c24:#f0944a;--c26:#6aa6f5;--good:#5fd29a;--warn:#f0b950;--bad:#ff8b7d;--neutral:#9aa4b6;
--goodbg:#173626;--warnbg:#3b2f12;--badbg:#401e1a;--neutralbg:#232b3a}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);
font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
.wrap{max-width:1240px;margin:0 auto;padding:20px 20px 48px}
header{display:flex;flex-wrap:wrap;justify-content:space-between;align-items:flex-end;gap:12px;margin-bottom:16px}
h1{font-size:22px;margin:0}h2{font-size:16px;margin:0 0 4px}.sub{color:var(--muted);font-size:12.5px}
.chips{display:flex;gap:8px;flex-wrap:wrap}.chip{border:1px solid var(--line);background:var(--card);border-radius:999px;padding:4px 12px;font-size:12.5px}
.chip b{font-weight:600}.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;background:var(--neutral)}
.dot.good{background:var(--good)}.dot.bad{background:var(--bad)}
.banner{border-radius:8px;padding:10px 14px;margin-bottom:14px;border:1px solid var(--line)}
.banner.bad{background:var(--badbg);border-color:var(--bad);color:var(--ink)}.banner.warn{background:var(--warnbg);border-color:var(--warn)}
.banner ul{margin:4px 0 0 18px;padding:0}
.hero{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-bottom:16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.hero .card .v{font-size:28px;font-weight:650;line-height:1.1}.hero .card .l{color:var(--muted);font-size:12.5px;margin-top:4px}
.v.good{color:var(--good)}.v.warn{color:var(--warn)}.v.bad{color:var(--bad)}
section.card{margin-bottom:16px}.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:16px}
table{width:100%;border-collapse:collapse;margin-top:8px}th,td{padding:6px 8px;text-align:left;border-bottom:1px solid var(--line);vertical-align:middle}
th{font-size:12px;color:var(--muted);font-weight:600;text-transform:none}td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}
tr:last-child td{border-bottom:0}.c24{color:var(--c24);font-weight:600}.c26{color:var(--c26);font-weight:600}
.track{background:var(--neutralbg);border-radius:4px;height:8px;min-width:70px}.fill{height:8px;border-radius:4px}
.fill.f24{background:var(--c24)}.fill.f26{background:var(--c26)}.fill.fgood{background:var(--good)}.fill.fwarn{background:var(--warn)}.fill.fbad{background:var(--bad)}
.pill{display:inline-block;border-radius:999px;padding:1px 9px;font-size:12px;font-weight:600;white-space:nowrap}
.pill.good{background:var(--goodbg);color:var(--good)}.pill.warn{background:var(--warnbg);color:var(--warn)}
.pill.bad{background:var(--badbg);color:var(--bad)}.pill.neutral{background:var(--neutralbg);color:var(--neutral)}
.stack{display:flex;height:22px;border-radius:5px;overflow:hidden;margin:10px 0 6px;background:var(--neutralbg)}
.stack div{height:100%}.legend{display:flex;flex-wrap:wrap;gap:6px 14px;font-size:12.5px}
.sw{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px}
.sw.good,.stack .good{background:var(--good)}.sw.warn,.stack .warn{background:var(--warn)}.sw.bad,.stack .bad{background:var(--bad)}
.sw.neutral,.stack .neutral{background:#98a2b3}
details{border:1px solid var(--line);border-radius:8px;margin-top:10px;padding:2px 12px;background:var(--card)}
summary{cursor:pointer;padding:8px 0;font-weight:600}.muted{color:var(--muted)}
svg text{fill:var(--muted);font-size:11px}svg .grid{stroke:var(--line)}svg .l24{stroke:var(--c24);fill:none;stroke-width:2}
svg .l26{stroke:var(--c26);fill:none;stroke-width:2}svg .p24{fill:var(--c24)}svg .p26{fill:var(--c26)}svg .lg{stroke:var(--good);fill:none;stroke-width:2}svg .pg{fill:var(--good)}
.key{display:flex;gap:14px;font-size:12.5px;margin-top:4px}footer{color:var(--muted);font-size:12px;margin-top:20px}
.scroll{overflow-x:auto}.scroll.tall{max-height:340px;overflow:auto;border-top:1px solid var(--line)}
.scroll.tall th{position:sticky;top:0;background:var(--card);box-shadow:0 1px 0 var(--line)}
"""


def esc(x):
    return html.escape("" if x is None else str(x))


def num(x, suffix=""):
    if x is None:
        return "&mdash;"
    if isinstance(x, float):
        return "{:,.1f}{}".format(x, suffix)
    return "{:,}{}".format(x, suffix)


def pill(status):
    label, cls, tip = JOB_STATUS_META[status]
    return '<span class="pill %s" title="%s">%s</span>' % (cls, esc(tip), esc(label))


def bar(value, maxv, cls):
    w = 0 if not maxv or not value else min(100.0, value * 100.0 / maxv)
    return '<div class="track"><div class="fill %s" style="width:%.1f%%"></div></div>' % (cls, w)


def nice_max(v):
    if v <= 5:
        return 5
    mag = 10 ** (len(str(int(v))) - 1)
    for m in (1, 2, 2.5, 5, 10):
        if v <= m * mag:
            return m * mag
    return 10 * mag


def svg_lines(labels, series, width=620, height=230, ymax=None, unit=""):
    """series: list of (name, line_class, point_class, values)."""
    pl, pr, pt, pb = 46, 14, 12, 28
    vals = [v for _, _, _, vs in series for v in vs if v is not None]
    top = ymax if ymax else nice_max(max(vals + [1]))
    iw, ih = width - pl - pr, height - pt - pb
    n = max(len(labels), 2)
    X = lambda i: pl + iw * i / (n - 1)
    Y = lambda v: pt + ih - ih * min(v, top) / top
    out = ['<svg viewBox="0 0 %d %d" width="100%%" role="img" aria-label="line chart">' % (width, height)]
    for frac in (0, 0.5, 1):
        y = pt + ih - ih * frac
        out.append('<line class="grid" x1="%d" x2="%d" y1="%.1f" y2="%.1f"/>' % (pl, width - pr, y, y))
        out.append('<text x="%d" y="%.1f" text-anchor="end">%s%s</text>' % (pl - 6, y + 4, num(top * frac).replace(".0", ""), unit))
    for i in sorted({0, len(labels) // 2, len(labels) - 1}):
        if labels:
            out.append('<text x="%.1f" y="%d" text-anchor="middle">%s</text>' % (X(i), height - 8, esc(labels[i])))
    for name, lc, pc, vs in series:
        pts = [(X(i), Y(v)) for i, v in enumerate(vs) if v is not None]
        if len(pts) > 1:
            out.append('<polyline class="%s" points="%s"/>' % (lc, " ".join("%.1f,%.1f" % p for p in pts)))
        for i, v in enumerate(vs):
            if v is not None:
                out.append('<circle class="%s" cx="%.1f" cy="%.1f" r="3"><title>%s %s: %s%s</title></circle>' % (
                    pc, X(i), Y(v), esc(name), esc(labels[i]), num(v), unit))
    out.append("</svg>")
    return "".join(out)


def table(headers, rows, num_cols=(), tall=False):
    h = "".join('<th class="%s">%s</th>' % ("n" if i in num_cols else "", t) for i, t in enumerate(headers))
    body = "".join("<tr>%s</tr>" % "".join(
        '<td class="%s">%s</td>' % ("n" if i in num_cols else "", c) for i, c in enumerate(r)) for r in rows)
    return '<div class="scroll%s"><table><thead><tr>%s</tr></thead><tbody>%s</tbody></table></div>' % (" tall" if tall else "", h, body)


def ratio_cells(v24, v26):
    if v24 is None or v26 is None:
        return "&mdash;", "&mdash;"
    d = v26 - v24
    r = "%.0f%%" % (v26 * 100.0 / v24) if v24 else "&mdash;"
    return ("%+d" % d if isinstance(d, int) else "%+.1f" % d), r


def render_html(ctx):
    s24, s26, cmp_, history = ctx["s24"], ctx["s26"], ctx["cmp"], ctx["history"]
    args, now = ctx["args"], ctx["now"]
    a24, a26 = s24.get("activity") or {}, s26.get("activity") or {}
    sc24, sc26 = s24.get("schedules") or {}, s26.get("schedules") or {}
    u24, u26 = s24.get("users") or {}, s26.get("users") or {}
    c24, c26 = s24.get("counts") or {}, s26.get("counts") or {}
    jobs = cmp_.get("jobs") or {}
    jc = jobs.get("counts") or {}
    P = []

    def chip(s, name):
        ok = s.get("authenticated")
        return '<span class="chip"><span class="dot %s"></span><b>%s</b> %s &middot; %s</span>' % (
            "good" if ok else "bad", name, esc(s.get("version") or "unreachable"), esc(s.get("host")))

    P.append('<header><div><h1>AAP 2.4 &rarr; 2.6 Migration Dashboard</h1><div class="sub">Generated %s UTC &middot; '
             'history window %d days &middot; &ldquo;active&rdquo; = logged in within %d days</div></div>'
             '<div class="chips">%s%s</div></header>' % (
                 now.strftime("%Y-%m-%d %H:%M"), args.history_days, args.active_days,
                 chip(s24, "AAP 2.4"), chip(s26, "AAP 2.6")))

    problems = ["<b>%s</b>: %s" % (esc(s["label"]), esc(e)) for s in (s24, s26) for e in s["errors"]]
    if problems:
        P.append('<div class="banner %s"><b>Data collection problems</b> - figures below may be incomplete.<ul>%s</ul></div>' % (
            "bad" if not (s24.get("authenticated") and s26.get("authenticated")) else "warn",
            "".join("<li>%s</li>" % p for p in problems)))

    # ---- hero cards ----
    def card(value, label, cls=""):
        return '<div class="card"><div class="v %s">%s</div><div class="l">%s</div></div>' % (cls, value, label)

    na = jc.get("not_migrated_active", 0)
    dual = jc.get("dual_scheduled", 0)
    wait = jc.get("awaiting_cutover", 0)
    ad = cmp_.get("adoption") or {}
    P.append('<div class="hero">%s</div>' % "".join([
        card(num(cmp_.get("overall_pct"), "%"), "Overall configuration parity (objects in both / objects in 2.4)"),
        card("%s / %s" % (num(jobs.get("migrated")), num(jobs.get("total24"))) if jobs else "&mdash;",
             "Unique jobs migrated (%s)" % num(jobs.get("migrated_pct"), "%")),
        card(num(na) if jobs else "&mdash;", "Active in 2.4 but NOT in 2.6", ("bad" if na else "good") if jobs else ""),
        card(num(wait) if jobs else "&mdash;", "Migrated, awaiting cutover", ("warn" if wait else "good") if jobs else ""),
        card(num(dual) if jobs else "&mdash;", "Dual-scheduled (duplicate-run risk)", ("bad" if dual else "good") if jobs else ""),
        card(num(ad.get("logged26_pct"), "%") if ad else "&mdash;",
             "2.4-active users who have logged into 2.6 (%s of %s)" % (num(ad.get("logged26")), num(ad.get("active24")))),
    ]))

    # ---- core metrics ----
    def mrow(label, v24, v26, hint=""):
        d, r = ratio_cells(v24, v26)
        return ['%s<div class="sub">%s</div>' % (esc(label), esc(hint)) if hint else esc(label),
                '<span class="c24">%s</span>' % num(v24), '<span class="c26">%s</span>' % num(v26), d, r]

    live24, live26 = s24.get("live") or {}, s26.get("live") or {}
    rows = [
        mrow("Jobs run, last 24h", a24.get("jobs_24h"), a26.get("jobs_24h"),
             "playbook jobs created in the last 24h, incl. jobs launched by workflows"),
        mrow("Workflow runs, last 24h", a24.get("workflow_24h"), a26.get("workflow_24h")),
        mrow("Jobs running right now", live24.get("running"), live26.get("running"),
             "pending now: %s (2.4) / %s (2.6)" % (live24.get("pending", "-"), live26.get("pending", "-"))),
        mrow("Active scheduled jobs", sc24.get("enabled_jobs"), sc26.get("enabled_jobs"),
             "enabled schedules on job & workflow templates"),
        mrow("Active users", u24.get("active"), u26.get("active"),
             "logged in within %d days; total %s (2.4) / %s (2.6)" % (args.active_days, u24.get("total", "-"), u26.get("total", "-"))),
        mrow("Organizations", c24.get("organizations"), c26.get("organizations")),
        mrow("Projects", c24.get("projects"), c26.get("projects")),
        mrow("Unique jobs defined", c24.get("unique_defined"), c26.get("unique_defined"),
             "job templates + workflow templates, each counted once"),
        mrow("Unique jobs with an enabled schedule", c24.get("unique_scheduled"), c26.get("unique_scheduled")),
        mrow("Unique jobs that ran, last %d days" % args.history_days, a24.get("unique_ran_window"), a26.get("unique_ran_window"),
             "a job that ran 500 times counts as 1"),
        mrow("Unique jobs that ran, last 24h", a24.get("unique_ran_24h"), a26.get("unique_ran_24h")),
    ]
    P.append('<section class="card"><h2>Core metrics</h2><div class="sub">Activity should shift from 2.4 to 2.6 over '
             'the migration; configuration counts in 2.6 should approach 100%% of 2.4.</div>%s</section>' % table(
                 ["Metric", '<span class="c24">AAP 2.4</span>', '<span class="c26">AAP 2.6</span>', "Delta (2.6-2.4)", "2.6 / 2.4"],
                 rows, num_cols=(1, 2, 3, 4)))

    # ---- cutover status ----
    if jobs:
        total = jobs["total24"] or 1
        segs = "".join('<div class="%s" style="width:%.2f%%" title="%s: %d"></div>' % (
            JOB_STATUS_META[k][1], jc.get(k, 0) * 100.0 / total, esc(JOB_STATUS_META[k][0]), jc.get(k, 0))
            for k in JOB_STATUS_ORDER if jc.get(k))
        legend = "".join('<span><span class="sw %s"></span>%s <b>%d</b></span>' % (
            JOB_STATUS_META[k][1], esc(JOB_STATUS_META[k][0]), jc.get(k, 0)) for k in JOB_STATUS_ORDER)
        details = ""
        for k in JOB_STATUS_ORDER:
            subset = [r for r in jobs["rows"] if r["status"] == k]
            if not subset or k in ("dormant",):
                continue
            show = subset[:args.max_rows]
            trs = [[esc(r["kind"]), esc(r["org"]), esc(r["name"]), num(r["sched24"]),
                    num(r["sched26"]), num(r["runs24"]), num(r["runs26"]),
                    esc((r["last24"] or "")[:10]), esc((r["last26"] or "")[:10])] for r in show]
            more = "" if len(subset) <= len(show) else '<div class="sub">Showing %d of %d - full list in the CSV.</div>' % (len(show), len(subset))
            op = " open" if k in ("not_migrated_active", "dual_scheduled") else ""
            details += '<details%s><summary>%s &mdash; %d</summary><div class="sub">%s</div>%s%s</details>' % (
                op, esc(JOB_STATUS_META[k][0]), len(subset), esc(JOB_STATUS_META[k][2]),
                table(["Type", "Organization", "Name", "2.4 sched", "2.6 sched", "2.4 runs", "2.6 runs", "2.4 last run", "2.6 last run"],
                      trs, num_cols=(3, 4, 5, 6), tall=True), more)
        P.append('<section class="card"><h2>Unique job cutover status</h2><div class="sub">Each job or workflow template '
                 'in 2.4 is counted once and classified by what exists and what is active on each platform '
                 '(active = enabled schedule or ran in the last %d days). %d unique jobs exist only in 2.6.</div>'
                 '<div class="stack">%s</div><div class="legend">%s</div>%s</section>' % (
                     args.history_days, jobs.get("new26", 0), segs, legend, details))

    # ---- cadence ----
    def bucket_rows(keys, d24, d26, note):
        mx = max([d24.get(k, 0) for k in keys] + [d26.get(k, 0) for k in keys] + [1])
        out = []
        for k in keys:
            if not d24.get(k) and not d26.get(k):
                continue
            out.append([esc(BUCKET_LABEL[k]),
                        '<span class="c24">%d</span>' % d24.get(k, 0), bar(d24.get(k, 0), mx, "f24"),
                        '<span class="c26">%d</span>' % d26.get(k, 0), bar(d26.get(k, 0), mx, "f26")])
        out.append(["<b>Total</b>", "<b>%d</b>" % sum(d24.values()), "", "<b>%d</b>" % sum(d26.values()), ""])
        return table([note, "2.4", "", "2.6", ""], out, num_cols=(1, 3))

    sched_keys = [k for k, _ in BUCKETS if k not in ("single_run",)]
    obs_keys = [k for k, _ in BUCKETS if k not in ("one_time",)]
    P.append('<div class="grid2"><section class="card"><h2>Unique jobs by schedule cadence</h2>'
             '<div class="sub">Job/workflow templates with at least one enabled schedule, bucketed by their most frequent '
             'schedule (parsed from the RRULE). Weekday-only schedules count as daily.</div>%s</section>'
             '<section class="card"><h2>Unique jobs by observed cadence</h2><div class="sub">Based on the median gap between '
             'actual runs in the last %d days. Includes manually launched jobs. Monthly jobs need a window of 35+ days to show.</div>%s</section></div>' % (
                 bucket_rows(sched_keys, sc24.get("buckets_unique") or {}, sc26.get("buckets_unique") or {}, "Scheduled cadence"),
                 args.history_days,
                 bucket_rows(obs_keys, a24.get("observed_buckets") or {}, a26.get("observed_buckets") or {}, "Observed cadence")))

    # ---- parity ----
    prow = []
    for c in cmp_["categories"]:
        if not c["available"]:
            prow.append([esc(c["label"]), num(c["n24"]), num(c["n26"]), "&mdash;", "&mdash;", "&mdash;", "data unavailable"])
            continue
        p = c["pct"]
        cls = "fgood" if p is not None and p >= 95 else ("fwarn" if p is not None and p >= 60 else "fbad")
        prow.append([esc(c["label"]), '<span class="c24">%d</span>' % c["n24"], '<span class="c26">%d</span>' % c["n26"],
                     str(c["both"]), "<b>%d</b>" % c["pending"] if c["pending"] else "0", str(c["new26"]),
                     '<div style="display:flex;gap:8px;align-items:center"><div style="flex:1">%s</div>'
                     '<span style="min-width:48px;text-align:right">%s</span></div>' % (bar(p or 0, 100, cls), num(p, "%"))])
    pend_details = ""
    by_cat = {}
    for r in cmp_["pending_rows"]:
        by_cat.setdefault(r["category"], []).append(r)
    for cat, items in by_cat.items():
        if cat.startswith("Unique job"):
            continue
        show = items[:args.max_rows]
        pend_details += '<details><summary>%s &mdash; %d</summary>%s%s</details>' % (
            esc(cat), len(items),
            table(["Organization", "Name", "Detail"], [[esc(r["org"]), esc(r["name"]), esc(r["detail"])] for r in show], tall=True),
            "" if len(items) <= len(show) else '<div class="sub">Showing %d of %d - full list in the CSV.</div>' % (len(show), len(items)))
    P.append('<section class="card"><h2>Migration parity by object type</h2><div class="sub">Objects are matched by '
             'organization + name (IDs differ between installs). Built-in/sample objects are excluded. '
             '&ldquo;Pending&rdquo; = in 2.4 but not found in 2.6; &ldquo;New in 2.6&rdquo; = only in 2.6.</div>%s%s</section>' % (
                 table(["Object type", '<span class="c24">2.4</span>', '<span class="c26">2.6</span>', "In both", "Pending", "New in 2.6", "Progress"],
                       prow, num_cols=(1, 2, 3, 4, 5)), pend_details))

    # ---- activity ----
    day_labels = sorted((a26.get("daily") or a24.get("daily") or {}).keys())
    chart = svg_lines([d[5:] for d in day_labels], [
        ("AAP 2.4", "l24", "p24", [(a24.get("daily") or {}).get(d, [None])[0] for d in day_labels]),
        ("AAP 2.6", "l26", "p26", [(a26.get("daily") or {}).get(d, [None])[0] for d in day_labels]),
    ]) if day_labels else '<div class="muted">No history available.</div>'

    def status_rows(a):
        st = a.get("status_24h") or {}
        return st

    st24, st26 = status_rows(a24), status_rows(a26)
    status_keys = sorted(set(st24) | set(st26), key=lambda k: (k != "successful", k))
    srows = [[esc(k), '<span class="c24">%d</span>' % st24.get(k, 0), '<span class="c26">%d</span>' % st26.get(k, 0)] for k in status_keys]
    srows.append(["<b>Failure rate (failed + error)</b>", num(a24.get("failure_rate_24h"), "%"), num(a26.get("failure_rate_24h"), "%")])
    srows.append(["Avg / p95 duration (window, seconds)",
                  "%s / %s" % (num(a24.get("elapsed_avg")), num(a24.get("elapsed_p95"))),
                  "%s / %s" % (num(a26.get("elapsed_avg")), num(a26.get("elapsed_p95")))])
    srows.append(["People who launched jobs (window)", num(a24.get("launchers")), num(a26.get("launchers"))])
    l24, l26 = a24.get("launch_24h") or {}, a26.get("launch_24h") or {}
    lrows = [[esc(k), '<span class="c24">%d</span>' % l24.get(k, 0), '<span class="c26">%d</span>' % l26.get(k, 0)]
             for k in sorted(set(l24) | set(l26), key=lambda k: -(l24.get(k, 0) + l26.get(k, 0)))]
    P.append('<div class="grid2"><section class="card"><h2>Jobs run per day (last 14 days)</h2>'
             '<div class="sub">UTC days; the final point is today so far (partial).</div><div class="key"><span class="c24">&#9679; AAP 2.4</span><span class="c26">&#9679; AAP 2.6</span></div>%s</section>'
             '<section class="card"><h2>Job outcomes, last 24h</h2>%s<h2 style="margin-top:14px">Launch types, last 24h</h2>%s</section></div>' % (
                 chart, table(["Status", '<span class="c24">2.4</span>', '<span class="c26">2.6</span>'], srows, num_cols=(1, 2)),
                 table(["Launch type", '<span class="c24">2.4</span>', '<span class="c26">2.6</span>'], lrows, num_cols=(1, 2))))

    def top_table(a):
        t = a.get("top_templates") or []
        return table(["Template", "Runs"], [["%s<div class='sub'>%s</div>" % (esc(x["name"]), esc(x["org"] or "no org")), str(x["runs"])]
                                            for x in t], num_cols=(1,)) if t else '<div class="muted">No runs in window.</div>'
    P.append('<div class="grid2"><section class="card"><h2><span class="c24">AAP 2.4</span> - busiest templates (%dd)</h2>%s</section>'
             '<section class="card"><h2><span class="c26">AAP 2.6</span> - busiest templates (%dd)</h2>%s</section></div>' % (
                 args.history_days, top_table(a24), args.history_days, top_table(a26)))

    # ---- adoption + health ----
    if ad:
        P.append('<section class="card"><h2>User adoption</h2><div class="sub">Of the users who logged into 2.4 within '
                 '%d days, how many exist on 2.6 and have actually logged in there.</div>%s</section>' % (
                     args.active_days, table(["Measure", "Users", "% of 2.4-active"], [
                         ["Active in 2.4", str(ad["active24"]), "100%"],
                         ["...exist in 2.6", str(ad["exist26"]), num(ad["exist26_pct"], "%")],
                         ["...have logged into 2.6", "<b>%d</b>" % ad["logged26"], num(ad["logged26_pct"], "%")],
                         ["...missing from 2.6", "<b>%d</b>" % ad["missing26"] if ad["missing26"] else "0", ""],
                     ], num_cols=(1, 2))))

    def cap(s, k, f=None):
        v = (s.get("capacity") or {}).get(k)
        return num(v) if f is None else f(v)

    def projfail(s):
        ps = s.get("project_status") or {}
        return sum(v for k, v in ps.items() if k in ("failed", "error"))

    hrows = [
        ["Instances / nodes", cap(s24, "instances"), cap(s26, "instances")],
        ["Capacity used (forks)", num((s24.get("capacity") or {}).get("used_pct"), "%"), num((s26.get("capacity") or {}).get("used_pct"), "%")],
        ["Nodes reporting errors / disabled", "%s / %s" % (cap(s24, "with_errors"), cap(s24, "disabled")),
         "%s / %s" % (cap(s26, "with_errors"), cap(s26, "disabled"))],
        ["Projects with failed sync", num(projfail(s24)), num(projfail(s26))],
        ["Enabled schedules with no next run (stale)", num(sc24.get("dead")), num(sc26.get("dead"))],
        ["Disabled schedules", num(sc24.get("disabled")), num(sc26.get("disabled"))],
        ["Management/project/inventory schedules enabled", num(sc24.get("enabled_other")), num(sc26.get("enabled_other"))],
        ["Users never logged in", num(u24.get("never_logged_in")), num(u26.get("never_logged_in"))],
        ["Superusers", num(u24.get("superusers")), num(u26.get("superusers"))],
        ["Teams", num(c24.get("teams")), num(c26.get("teams"))],
        ["Inventories / hosts", "%s / %s" % (num(c24.get("inventories")), num(c24.get("hosts"))),
         "%s / %s" % (num(c26.get("inventories")), num(c26.get("hosts")))],
        ["Credentials", num(c24.get("credentials")), num(c26.get("credentials"))],
        ["Job / workflow templates", "%s / %s" % (num(c24.get("job_templates")), num(c24.get("workflow_job_templates"))),
         "%s / %s" % (num(c26.get("job_templates")), num(c26.get("workflow_job_templates")))],
        ["Notification templates", num(c24.get("notification_templates")), num(c26.get("notification_templates"))],
        ["Execution environments", num(c24.get("execution_environments")), num(c26.get("execution_environments"))],
    ]
    P.append('<section class="card"><h2>Health, capacity and inventory counts</h2>%s</section>' % table(
        ["Measure", '<span class="c24">AAP 2.4</span>', '<span class="c26">AAP 2.6</span>'], hrows, num_cols=(1, 2)))

    # ---- trend ----
    if len(history) >= 2:
        labels = [h["date"][5:] for h in history]
        t1 = svg_lines(labels, [("Config parity %", "lg", "pg", [h.get("overall_pct") for h in history]),
                                ("Unique jobs migrated %", "l26", "p26", [h.get("unique_migrated_pct") for h in history])],
                       ymax=100, unit="%")
        t2 = svg_lines(labels, [("2.4 jobs/24h", "l24", "p24", [(h.get("jobs_24h") or [None, None])[0] for h in history]),
                                ("2.6 jobs/24h", "l26", "p26", [(h.get("jobs_24h") or [None, None])[1] for h in history])])
        P.append('<div class="grid2"><section class="card"><h2>Trend: migration progress</h2><div class="key">'
                 '<span style="color:var(--good)">&#9679; Config parity</span><span class="c26">&#9679; Unique jobs migrated</span></div>%s</section>'
                 '<section class="card"><h2>Trend: jobs run in 24h</h2><div class="key"><span class="c24">&#9679; AAP 2.4</span>'
                 '<span class="c26">&#9679; AAP 2.6</span></div>%s</section></div>' % (t1, t2))
    else:
        P.append('<section class="card"><h2>Trend</h2><div class="muted">Trend charts appear after the script has run on at '
                 'least two different days (history.json is updated on every run).</div></section>')

    notes = [n for s in (s24, s26) for n in s["notes"]]
    excl = ([] if args.no_default_excludes else DEFAULT_EXCLUDES) + (args.exclude_regex or [])
    P.append('<footer><b>Notes.</b> %s<br>Excluded from parity (case-insensitive name patterns, plus objects flagged managed): '
             '<code>%s</code>. Org renames applied to 2.4: %s. Users are taken from the %s on 2.6 (%s on 2.4). '
             'API requests this run: %d (2.4) / %d (2.6). Script v%s.</footer>' % (
                 esc(" ".join(notes)) or "None.", esc(" | ".join(excl)) or "none",
                 esc(", ".join(args.org_rename or [])) or "none", esc(s26.get("users_source", "n/a")),
                 esc(s24.get("users_source", "n/a")), s24.get("request_count", 0), s26.get("request_count", 0), VERSION))

    return ('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">'
            '<title>AAP 2.4 to 2.6 Migration Dashboard</title><style>%s</style></head><body><div class="wrap">%s</div></body></html>' % (
                CSS, "".join(P)))


# --------------------------------------------------------------------------- #
# Console summary, CLI, main
# --------------------------------------------------------------------------- #

def console_summary(s24, s26, cmp_, args):
    a24, a26 = s24.get("activity") or {}, s26.get("activity") or {}
    rows = [
        ("Jobs run, last 24h", a24.get("jobs_24h"), a26.get("jobs_24h")),
        ("Running now", (s24.get("live") or {}).get("running"), (s26.get("live") or {}).get("running")),
        ("Active scheduled jobs", (s24.get("schedules") or {}).get("enabled_jobs"), (s26.get("schedules") or {}).get("enabled_jobs")),
        ("Active users (%dd)" % args.active_days, (s24.get("users") or {}).get("active"), (s26.get("users") or {}).get("active")),
        ("Organizations", (s24.get("counts") or {}).get("organizations"), (s26.get("counts") or {}).get("organizations")),
        ("Projects", (s24.get("counts") or {}).get("projects"), (s26.get("counts") or {}).get("projects")),
        ("Unique jobs defined", (s24.get("counts") or {}).get("unique_defined"), (s26.get("counts") or {}).get("unique_defined")),
        ("Unique jobs ran (%dd)" % args.history_days, a24.get("unique_ran_window"), a26.get("unique_ran_window")),
    ]
    out = ["", "%-28s %10s %10s" % ("Metric", "AAP 2.4", "AAP 2.6"), "-" * 50]
    out += ["%-28s %10s %10s" % (n, "-" if a is None else a, "-" if b is None else b) for n, a, b in rows]
    out.append("")
    out.append("Config parity overall: %s%%" % (cmp_.get("overall_pct") if cmp_.get("overall_pct") is not None else "n/a"))
    jobs = cmp_.get("jobs") or {}
    if jobs:
        out.append("Unique jobs migrated: %d of %d (%s%%)" % (jobs["migrated"], jobs["total24"], jobs["migrated_pct"]))
        for k in JOB_STATUS_ORDER:
            if jobs["counts"].get(k):
                out.append("  %-40s %d" % (JOB_STATUS_META[k][0], jobs["counts"][k]))
    return "\n".join(out)


def parse_args(argv=None):
    env = os.environ.get
    p = argparse.ArgumentParser(description="Daily AAP 2.4 vs 2.6 migration dashboard.",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--host24", default=env("AAP24_HOST"), help="AAP 2.4 controller host (env AAP24_HOST)")
    p.add_argument("--host26", default=env("AAP26_HOST"), help="AAP 2.6 gateway host (env AAP26_HOST)")
    p.add_argument("--prefix24", default="/api/v2", help="controller API prefix on 2.4")
    p.add_argument("--prefix26", default="/api/controller/v2", help="controller API prefix on 2.6 (via gateway)")
    p.add_argument("--gateway-prefix26", default="/api/gateway/v1",
                   help="gateway API prefix on 2.6 for users/orgs cross-check; empty string disables")
    p.add_argument("-o", "--output-dir", default="./aap_migration_dashboard")
    p.add_argument("--history-days", type=int, default=60,
                   help="job history window for unique-job and cadence analysis (35+ needed to see monthly jobs)")
    p.add_argument("--active-days", type=int, default=30, help="a user is active if last login is within N days")
    p.add_argument("--max-jobs", type=int, default=100000, help="cap on job history rows fetched per endpoint, newest first")
    p.add_argument("--max-objects", type=int, default=50000, help="cap on rows per configuration object list")
    p.add_argument("--max-rows", type=int, default=100, help="max rows per detail table in the HTML")
    p.add_argument("--exclude-regex", action="append", help="extra regex of names to ignore in parity (repeatable)")
    p.add_argument("--no-default-excludes", action="store_true", help="do not ignore built-in/sample objects")
    p.add_argument("--org-rename", action="append", metavar="OLD=NEW",
                   help="organization was renamed during migration (repeatable)")
    p.add_argument("--insecure", action="store_true", help="skip TLS certificate verification (not recommended)")
    p.add_argument("--ca-bundle", default=env("AAP_CA_BUNDLE"), help="CA bundle file for both platforms")
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--serve", type=int, metavar="PORT", help="after writing, serve the output directory on PORT")
    p.add_argument("--bind", default="127.0.0.1", help="address for --serve")
    p.add_argument("--json", action="store_true", help="print the summary as JSON on stdout")
    p.add_argument("-q", "--quiet", action="store_true")
    return p.parse_args(argv)


def make_client(label, host, prefix, gw, args, prefix_env):
    token = os.environ.get(prefix_env + "_TOKEN")
    user, pw = os.environ.get(prefix_env + "_USERNAME"), os.environ.get(prefix_env + "_PASSWORD")
    return Client(label, host, prefix, gw, token, user, pw, verify=not args.insecure,
                  ca_bundle=args.ca_bundle, timeout=args.timeout, retries=args.retries)


def main(argv=None):
    args = parse_args(argv)
    if not args.host24 or not args.host26:
        print("ERROR: both AAP 2.4 and 2.6 hosts are required (--host24/--host26 or AAP24_HOST/AAP26_HOST).", file=sys.stderr)
        return 2
    for env_prefix in ("AAP24", "AAP26"):
        if not (os.environ.get(env_prefix + "_TOKEN") or os.environ.get(env_prefix + "_USERNAME")):
            print("ERROR: set %s_TOKEN (or %s_USERNAME and %s_PASSWORD)." % (env_prefix, env_prefix, env_prefix), file=sys.stderr)
            return 2
    now = datetime.now(timezone.utc)
    c24 = make_client("AAP 2.4", args.host24, args.prefix24, "", args, "AAP24")
    c26 = make_client("AAP 2.6", args.host26, args.prefix26, args.gateway_prefix26, args, "AAP26")
    log("Collecting from AAP 2.4 and AAP 2.6 ...", args.quiet)
    with ThreadPoolExecutor(max_workers=2) as pool:
        f24 = pool.submit(collect_side, c24, args, now)
        f26 = pool.submit(collect_side, c26, args, now)
        s24, s26 = f24.result(), f26.result()
    usable = s24.get("authenticated") and s26.get("authenticated")
    cmp_ = compare_sides(s24, s26, args, now)

    out_dir = Path(args.output_dir)
    (out_dir / "snapshots").mkdir(parents=True, exist_ok=True)
    today = now.strftime("%Y-%m-%d")
    history = []
    if usable:
        history = update_history(out_dir, today, s24, s26, cmp_)
    else:
        try:
            history = json.loads((out_dir / "history.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            history = []
        log("WARNING: a platform was unusable; history.json and snapshots were not updated today.", args.quiet)
    page = render_html({"s24": s24, "s26": s26, "cmp": cmp_, "history": history, "args": args, "now": now})
    (out_dir / "index.html").write_text(page, encoding="utf-8")
    write_csv(out_dir / "migration_gap_latest.csv", cmp_["pending_rows"])
    if usable:
        (out_dir / "snapshots" / ("dashboard_%s.html" % today)).write_text(page, encoding="utf-8")
        write_csv(out_dir / "snapshots" / ("migration_gap_%s.csv" % today), cmp_["pending_rows"])
        snap = {"generated": iso(now), "version": VERSION, "aap24": strip_private(s24), "aap26": strip_private(s26),
                "comparison": strip_private({k: v for k, v in cmp_.items() if k != "pending_rows"})}
        (out_dir / "snapshots" / ("snapshot_%s.json" % today)).write_text(json.dumps(snap, indent=1, default=str), encoding="utf-8")

    if args.json:
        print(json.dumps(strip_private({"aap24": s24, "aap26": s26, "comparison": cmp_}), indent=1, default=str))
    elif not args.quiet:
        print(console_summary(s24, s26, cmp_, args))
        print("\nDashboard: %s" % (out_dir / "index.html").resolve())
        print("Gap list : %s" % (out_dir / "migration_gap_latest.csv").resolve())

    had_errors = bool(s24["errors"] or s26["errors"])
    code = 2 if not (s24.get("authenticated") or s26.get("authenticated")) else (1 if had_errors or not usable else 0)

    if args.serve:
        import functools
        import http.server
        handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(out_dir))
        print("Serving %s at http://%s:%d/  (Ctrl-C to stop)" % (out_dir, args.bind, args.serve))
        try:
            http.server.ThreadingHTTPServer((args.bind, args.serve), handler).serve_forever()
        except KeyboardInterrupt:
            pass
    return code


if __name__ == "__main__":
    sys.exit(main())
