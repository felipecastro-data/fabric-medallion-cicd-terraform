"""Production health check for the medallion lakehouse (run daily by .github/workflows/monitor.yml).

Looks at the latest ``pl_medallion`` run in a workspace and the latest data-quality run in ``lh_gold.dbo.dq_results``
and classifies the result:

* ``fail``  — last run failed or was cancelled, no successful run within ``MAX_RUN_AGE`` (the weekly schedule did not
  run), or any ``error`` data-quality check failed. The workflow opens/updates a GitHub issue.
* ``warn``  — only ``warn`` data-quality checks fired (reported, no issue).
* ``ok``    — healthy; the workflow closes an open alert issue.

    python tools/monitor_prod.py --workspace-id <prod-id> --report report.md   # writes status to $GITHUB_OUTPUT
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

MAX_RUN_AGE = timedelta(days=8)  # weekly schedule + a day of slack
PIPELINE = "pl_medallion"


@dataclass
class Health:
    status: str = "ok"
    findings: list[str] = field(default_factory=list)
    facts: list[str] = field(default_factory=list)

    def flag(self, status: str, finding: str) -> None:
        order = {"ok": 0, "warn": 1, "fail": 2}
        self.status = max(self.status, status, key=order.__getitem__)
        self.findings.append(f"**{status.upper()}** — {finding}")


def _parse(ts: str | None) -> datetime | None:
    """Fabric job timestamps are UTC with 7 fractional digits (``2026-10-23T15:05:40.5668356``); seconds suffice."""
    return datetime.fromisoformat(ts[:19]) if ts else None


def evaluate(runs: list[dict], dq_rows: list[dict], now: datetime) -> Health:
    """Pure health evaluation. ``runs``: Fabric job instances (newest first); ``dq_rows``: rows of ``dq_results``."""
    health = Health()

    if not runs:
        health.flag("fail", f"`{PIPELINE}` has never run in this workspace.")
    else:
        last = runs[0]
        health.facts.append(f"Last `{PIPELINE}` run: **{last['status']}** (started {last.get('startTimeUtc')} UTC)")
        if last["status"] in ("Failed", "Cancelled", "Deduped"):
            reason = (last.get("failureReason") or {}).get("message", "no failure reason reported")
            health.flag("fail", f"Last `{PIPELINE}` run {last['status'].lower()}: {reason[:600]}")
        succeeded = [r for r in runs if r["status"] == "Completed"]
        last_ok = _parse(succeeded[0].get("endTimeUtc")) if succeeded else None
        if last_ok is None:
            health.flag("fail", f"No successful `{PIPELINE}` run found.")
        elif now - last_ok > MAX_RUN_AGE:
            health.flag(
                "fail", f"Last successful run ended {last_ok:%Y-%m-%d %H:%M} UTC — older than {MAX_RUN_AGE.days} days."
            )
        else:
            health.facts.append(f"Last successful run ended {last_ok:%Y-%m-%d %H:%M} UTC")

    if not dq_rows:
        health.flag("fail", "No data-quality results found in `lh_gold.dbo.dq_results`.")
        return health

    latest_run = max(dq_rows, key=lambda r: r["checked_at"])["run_id"]
    latest = [r for r in dq_rows if r["run_id"] == latest_run]
    failed = [r for r in latest if r["outcome"] == "failed"]
    warnings = [r for r in latest if r["outcome"] == "warning"]
    passed = sum(r["outcome"] == "passed" for r in latest)
    health.facts.append(
        f"Latest data-quality run `{latest_run[:8]}`: {passed} passed, {len(warnings)} warnings, {len(failed)} failed"
    )
    for r in failed:
        health.flag(
            "fail", f"DQ `{r['check_name']}` ({r['period']}): observed {r['observed']}, expected {r['expected']}"
        )
    for r in warnings:
        health.flag("warn", f"DQ `{r['check_name']}` ({r['period']}): observed {r['observed']}")
    return health


def report(health: Health, workspace_id: str, now: datetime) -> str:
    icon = {"ok": "✅", "warn": "⚠️", "fail": "❌"}[health.status]
    lines = [
        f"## {icon} Production health: {health.status.upper()}",
        "",
        f"Checked {now:%Y-%m-%d %H:%M} UTC · workspace `{workspace_id}`",
        "",
    ]
    lines += [f"- {f}" for f in health.facts]
    if health.findings:
        lines += ["", "### Findings", "", *[f"- {f}" for f in health.findings]]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    import fabric_ops
    import read_onelake

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--report", default="health.md")
    args = parser.parse_args(argv)

    _, _, items = fabric_ops.req("GET", f"/workspaces/{args.workspace_id}/items?type=DataPipeline")
    pipeline = next((i["id"] for i in items["value"] if i["displayName"] == PIPELINE), None)
    runs = []
    if pipeline:
        _, _, jobs = fabric_ops.req("GET", f"/workspaces/{args.workspace_id}/items/{pipeline}/jobs/instances")
        runs = sorted(jobs.get("value", []), key=lambda j: j.get("startTimeUtc") or "", reverse=True)
    try:
        dq_rows = read_onelake.open_table(args.workspace_id, "lh_gold", "dq_results").to_pyarrow_table().to_pylist()
    except Exception as exc:  # noqa: BLE001 - surfaced as a finding
        print(f"could not read dq_results: {exc}", file=sys.stderr)
        dq_rows = []

    now = datetime.now(UTC).replace(tzinfo=None)
    health = evaluate(runs, dq_rows, now)
    text = report(health, args.workspace_id, now)
    with open(args.report, "w", encoding="utf-8") as fh:
        fh.write(text)
    print(text)
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as fh:
            fh.write(f"status={health.status}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
