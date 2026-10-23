from datetime import datetime

from monitor_prod import evaluate, report

NOW = datetime(2026, 10, 28, 14, 0)


def _run(status, start="2026-10-26T11:00:00.1234567", end="2026-10-26T11:20:00.1234567", reason=None):
    run = {"status": status, "startTimeUtc": start, "endTimeUtc": end}
    if reason:
        run["failureReason"] = {"message": reason}
    return run


def _dq(outcome="passed", run_id="r2", checked_at=datetime(2026, 10, 26, 11, 19), name="fact_trips.unique_ride_id"):
    return {"run_id": run_id, "checked_at": checked_at, "check_name": name, "period": "202609", "outcome": outcome,
            "observed": "0", "expected": "0"}  # fmt: skip


def test_healthy_run_and_dq_is_ok():
    health = evaluate([_run("Completed")], [_dq(), _dq(name="other")], NOW)
    assert health.status == "ok" and not health.findings


def test_failed_last_run_fails_with_its_reason():
    health = evaluate([_run("Failed", reason="Operation on target Gold build failed"), _run("Completed")], [_dq()], NOW)
    assert health.status == "fail"
    assert any("Gold build" in f for f in health.findings)


def test_stale_pipeline_fails_even_if_the_last_run_succeeded():
    old = _run("Completed", end="2026-10-12T11:20:00.0000000")
    health = evaluate([old], [_dq()], NOW)
    assert health.status == "fail" and any("older than 8 days" in f for f in health.findings)


def test_only_the_latest_dq_run_counts():
    rows = [_dq("failed", run_id="r1", checked_at=datetime(2026, 10, 19)), _dq("passed", run_id="r2")]
    assert evaluate([_run("Completed")], rows, NOW).status == "ok"


def test_dq_error_fails_and_warning_only_warns():
    assert evaluate([_run("Completed")], [_dq("failed")], NOW).status == "fail"
    warned = evaluate([_run("Completed")], [_dq("warning")], NOW)
    assert warned.status == "warn" and len(warned.findings) == 1


def test_never_run_and_missing_dq_fail():
    health = evaluate([], [], NOW)
    assert health.status == "fail" and len(health.findings) == 2


def test_report_shows_status_and_findings():
    text = report(evaluate([_run("Completed")], [_dq("warning")], NOW), "ws-1", NOW)
    assert text.startswith("## ⚠️ Production health: WARN") and "### Findings" in text


def test_a_crashing_check_still_reports_fail(monkeypatch, tmp_path):
    import fabric_ops
    import monitor_prod

    def broken_api(*args, **kwargs):
        raise SystemExit("GET /workspaces/ws/items -> 503 Service Unavailable")

    monkeypatch.setattr(fabric_ops, "req", broken_api)
    output, out_file = tmp_path / "report.md", tmp_path / "github_output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(out_file))

    assert monitor_prod.main(["--workspace-id", "ws", "--report", str(output)]) == 0
    assert "status=fail" in out_file.read_text()
    assert "could not collect data" in output.read_text() and "503" in output.read_text()
