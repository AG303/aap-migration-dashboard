# AAP 2.4 vs 2.6 Migration Dashboard

`aap_migration_dashboard.py` queries both platforms' REST APIs, compares them, and writes a
self-contained HTML dashboard you can review daily, plus a CSV of everything still to migrate.
Python 3.8+, standard library only (no pip installs, no CDN, works air-gapped).

## Quick start

```bash
export AAP24_HOST=aap24.example.com        # controller (AAP 2.4)
export AAP24_TOKEN=...                     # personal access token, read access to all orgs
export AAP26_HOST=aap26.example.com        # platform gateway (AAP 2.6)
export AAP26_TOKEN=...                     # gateway/controller token, read access to all orgs

python3 aap_migration_dashboard.py -o /var/www/html/aap-migration
python3 aap_migration_dashboard.py --serve 8080     # or view locally at http://127.0.0.1:8080
```

Use a system auditor or superuser token on each side; a normal user token only sees its own
organizations and the numbers will be understated. `AAPxx_USERNAME` / `AAPxx_PASSWORD` work as a
fallback. Secrets are read from environment variables only.

Daily cron (06:00), with the exports in a root-only env file:

```
0 6 * * * . /etc/aap-dashboard.env && /usr/bin/python3 /opt/aap/aap_migration_dashboard.py -o /var/www/html/aap-migration -q
```

Exit codes: `0` all queries succeeded, `1` partial (a query failed or one platform is down; the
dashboard is still written with a red banner), `2` neither platform usable or bad arguments.
Wire code 1 or 2 to your alerting.

## Outputs (in `--output-dir`)

| File | Purpose |
|---|---|
| `index.html` | The dashboard (latest run) |
| `migration_gap_latest.csv` | Everything pending: unmigrated objects, unmigrated users, jobs needing cutover |
| `history.json` | One entry per day; drives the trend charts (appear from the second day on) |
| `snapshots/snapshot_<date>.json` | Full daily snapshot, no secrets |
| `snapshots/dashboard_<date>.html`, `migration_gap_<date>.csv` | Archived copy of each day |

The HTML and CSV contain usernames and object names, so keep the output directory behind your
normal internal access controls.

## What the dashboard shows

**Core metrics (2.4 vs 2.6 side by side):** jobs run in the last 24h, workflow runs, jobs running
now, active scheduled jobs, active users, organizations, projects, and unique jobs.

**Unique jobs.** A job template or workflow template counts once no matter how many times it ran.
Shown as: defined, with an enabled schedule, ran in the last N days, ran in the last 24h.

**Cadence (daily / weekly / monthly and more), two ways:**
- Scheduled cadence: each enabled schedule's RRULE is parsed and the job is bucketed by its most
  frequent schedule. Weekday-only schedules count as daily.
- Observed cadence: the median gap between actual runs in the history window. Catches
  manually launched jobs that have no schedule. Needs `--history-days` of 35 or more to see monthly jobs.

Buckets: sub-daily, daily, several times a week, weekly, every 2-3 weeks, monthly, quarterly or
less, one-time, single run in window.

**Migration parity.** For organizations, users, teams, projects, inventories, credentials, job
templates, workflow templates, schedules, notification templates and execution environments:
count in 2.4, count in 2.6, in both, pending, new in 2.6, and a progress bar.

**Unique job cutover status** (the part to review each morning):

| Status | Meaning |
|---|---|
| Not migrated - active in 2.4 | Scheduled or running in 2.4, missing from 2.6. Highest priority |
| Dual-scheduled | Enabled schedules on both platforms; the automation may run twice |
| Migrated - awaiting cutover | In 2.6 but not yet scheduled or running there while 2.4 is still active |
| In use on both | Ran recently on both, not scheduled on both |
| Cutover complete | Active in 2.6, no longer active in 2.4 |
| Migrated - dormant | In both, neither active |
| Not migrated - inactive in 2.4 | No schedule or runs in the window; candidate to retire instead of migrate |

**Also included:** user adoption (2.4-active users who exist and have logged in on 2.6), job outcomes
and failure rate, launch types, average and p95 duration, people who launched jobs, jobs per day
for 14 days, busiest templates, instance capacity, projects with failed sync, stale schedules,
and trend charts of migration progress and job volume.

## Definitions and assumptions (please review)

- Everything is UTC. "Last 24h" is a rolling window; the last point on the daily chart is today so far.
- Jobs run = playbook jobs created in the window (`/jobs/`), including jobs launched by workflows.
  Workflow runs (`/workflow_jobs/`) are shown separately. Project and inventory syncs are not counted.
- Active user = `last_login` within `--active-days` (default 30). On 2.6 logins happen at the
  gateway, so the script reads users from `/api/gateway/v1/users/` and falls back to the controller
  (where `last_login` can be stale). A note appears on the dashboard if the two disagree.
- Active scheduled jobs = enabled schedules attached to job or workflow templates. Enabled project,
  inventory and management schedules are listed separately under Health.
- Objects are matched between platforms by organization plus name (case and whitespace normalised),
  because IDs differ between installs. If an organization was renamed during migration use
  `--org-rename OLD=NEW` or its children will show as pending.
- Built-in and sample objects are ignored (Demo items, Default org, default execution environments,
  cleanup schedules, anything flagged `managed`). Add `--exclude-regex` for more or
  `--no-default-excludes` to turn this off.
- Job history only goes back as far as your retention allows (`cleanup_jobs`). A job that has not
  run within the window and has no enabled schedule is treated as inactive.
- `--max-jobs` caps history rows per endpoint (newest first). If it triggers, a note is shown;
  24h figures are unaffected.
- Credentials are compared by name and type only; secrets are never read or stored.

## Verify against your environment

These parts were built from the documented API shapes and tested against the included mock, not
against your live systems. Run once and sanity-check the first dashboard:
- 2.6 API prefix is `/api/controller/v2` (override with `--prefix26`) and the gateway prefix is
  `/api/gateway/v1` (`--gateway-prefix26`; set to empty to skip the gateway).
- Job and workflow templates expose their organization in `summary_fields.organization`. If it is
  empty on one side, objects still match by name but same-named templates in different
  organizations will collide.
- Gateway user records include `last_login`.
- Compare the first run's counts for organizations, users and projects against the UI totals.

## Try it without touching real systems

```bash
python3 dev/mock_aap_server.py &            # fake 2.4 on :8424, fake 2.6 on :8426
export AAP24_HOST=http://127.0.0.1:8424 AAP24_TOKEN=mock-token
export AAP26_HOST=http://127.0.0.1:8426 AAP26_TOKEN=mock-token
python3 aap_migration_dashboard.py -o /tmp/demo --serve 8080
```

`dev/sample_dashboard.html` is a rendering produced from that mock data.

## Options

Run `python3 aap_migration_dashboard.py --help`. Useful ones: `--history-days`, `--active-days`,
`--insecure` (skip TLS verification) or `--ca-bundle`, `--exclude-regex`, `--org-rename`,
`--max-rows`, `--json` (print the full result as JSON), `--serve`, `-q`.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Red banner "Authentication failed: HTTP 401/403" | Token expired or lacks read access to all orgs |
| "HTTP 404 ... check the API prefix" | Wrong `--prefix24` / `--prefix26` |
| Certificate errors | Use `--ca-bundle`, or `--insecure` temporarily |
| 2.6 user counts differ from the UI | Gateway/controller user sync lag; see the note in the footer |
| Everything pending after an org rename | Add `--org-rename OLD=NEW` |
| Monthly jobs missing from observed cadence | Raise `--history-days` to 60 or more |
