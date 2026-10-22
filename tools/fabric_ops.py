"""Rehearse CD steps against a workspace with the signed-in Azure CLI user (feature workspaces, local runs).

python tools/fabric_ops.py update <workspace-id>                         # Update from Git (remote wins)
python tools/fabric_ops.py run <workspace-id> <notebook> '{"periods": "202609"}'  # RunNotebook job + poll
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

API = "https://api.fabric.microsoft.com/v1"
TERMINAL_JOB_STATES = ("Completed", "Failed", "Cancelled", "Deduped")


def _token() -> str:
    cmd = ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com"]
    return subprocess.check_output([*cmd, "--query", "accessToken", "-o", "tsv"], text=True).strip()


TOKEN = _token()


def req(method: str, url: str, body: dict | None = None) -> tuple[int, dict, dict | None]:
    url = url if url.startswith("http") else API + url
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    for attempt in range(8):
        request = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                raw = response.read()
                return response.status, dict(response.headers), json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raise SystemExit(f"{method} {url} -> {exc.code} {exc.read().decode()}") from exc
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:  # transient network
            print(f"  network error ({exc}); retry {attempt + 1}", flush=True)
            time.sleep(10)
    raise SystemExit(f"{method} {url}: network retries exhausted")


def wait_lro(headers: dict, label: str) -> dict:
    while True:
        time.sleep(int(headers.get("Retry-After", 5)))
        _, _, state = req("GET", headers["Location"])
        status = (state or {}).get("status")
        print(f"  {label}: {status}", flush=True)
        if status in ("Succeeded", "Completed"):
            return state
        if status in ("Failed", "Cancelled"):
            raise SystemExit(json.dumps(state))


def update_from_git(ws: str) -> None:
    _, _, status = req("GET", f"/workspaces/{ws}/git/status")
    if status["workspaceHead"] == status["remoteCommitHash"] and not status["changes"]:
        print("  already up to date", status["remoteCommitHash"][:7])
        return
    body = {
        "workspaceHead": status["workspaceHead"],
        "remoteCommitHash": status["remoteCommitHash"],
        "conflictResolution": {"conflictResolutionType": "Workspace", "conflictResolutionPolicy": "PreferRemote"},
        "options": {"allowOverrideItems": True},
    }
    code, headers, _ = req("POST", f"/workspaces/{ws}/git/updateFromGit", body)
    if code == 202:
        wait_lro(headers, "updateFromGit")
    print("  updated to", status["remoteCommitHash"][:7])


def run_notebook(ws: str, name: str, params: dict[str, str]) -> dict:
    _, _, items = req("GET", f"/workspaces/{ws}/items?type=Notebook")
    notebook = next(i["id"] for i in items["value"] if i["displayName"] == name)
    body = {"executionData": {"parameters": {k: {"value": v, "type": "string"} for k, v in params.items()}}}
    _, headers, _ = req("POST", f"/workspaces/{ws}/items/{notebook}/jobs/instances?jobType=RunNotebook", body)
    started = time.time()
    while True:
        time.sleep(20)
        _, _, job = req("GET", headers["Location"])
        print(f"  {name}: {job['status']} ({int(time.time() - started)}s)", flush=True)
        if job["status"] in TERMINAL_JOB_STATES:
            if job["status"] != "Completed":
                raise SystemExit(json.dumps(job.get("failureReason")))
            return job


if __name__ == "__main__":
    command, workspace = sys.argv[1], sys.argv[2]
    if command == "update":
        update_from_git(workspace)
    elif command == "run":
        run_notebook(workspace, sys.argv[3], json.loads(sys.argv[4]) if len(sys.argv) > 4 else {})
    else:
        raise SystemExit(__doc__)
