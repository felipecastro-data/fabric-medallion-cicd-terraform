"""Fabric CD operations via the REST API as the Azure CLI identity (user locally, service principal in CI).

python tools/fabric_ops.py update <workspace-id> [<expected-commit>]     # Update from Git (remote wins)
python tools/fabric_ops.py run <workspace-id> <notebook> '{"periods": "202609"}'  # RunNotebook job + poll
python tools/fabric_ops.py run-pipeline <workspace-id> pl_medallion '{"periods": "202607-202609"}'  # Pipeline job
python tools/fabric_ops.py git-credentials <workspace-id> <connection-id>   # SP Git access via shared connection
python tools/fabric_ops.py default-env <workspace-id> env_divvy              # workspace default Spark environment
python tools/fabric_ops.py deploy <pipeline-id> Development Test "<note>"    # deployment pipeline stage -> stage
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request

from fabric_auth import access_token  # fresh GitHub OIDC exchange in CI, az CLI locally

API = "https://api.fabric.microsoft.com/v1"
TERMINAL_JOB_STATES = ("Completed", "Failed", "Cancelled", "Deduped")


def req(method: str, url: str, body: dict | None = None) -> tuple[int, dict, dict | None]:
    url = url if url.startswith("http") else API + url
    data = json.dumps(body).encode() if body is not None else None
    for attempt in range(8):
        headers = {"Authorization": f"Bearer {access_token()}", "Content-Type": "application/json"}
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


def update_from_git(ws: str, expected_commit: str | None = None) -> None:
    """Update the workspace from its Git branch (remote wins).

    With ``expected_commit`` (CD passes ``GITHUB_SHA``) the sync refuses to apply any other commit: if the branch head
    moved on, a newer workflow run deploys it, and this run must not promote code its approval never covered.
    """
    _, _, st = req("GET", f"/workspaces/{ws}/git/status")
    remote = st["remoteCommitHash"]
    if expected_commit and remote != expected_commit:
        raise SystemExit(
            f"Branch head is {remote[:7]}, not this run's commit {expected_commit[:7]}: a newer commit reached the "
            "branch; its own deploy run will promote it. Refusing to deploy a revision this run did not build."
        )
    if st["workspaceHead"] == remote and not st["changes"]:
        print("  already up to date", remote[:7])
        return
    body = {
        "workspaceHead": st["workspaceHead"],
        "remoteCommitHash": remote,
        "conflictResolution": {"conflictResolutionType": "Workspace", "conflictResolutionPolicy": "PreferRemote"},
        "options": {"allowOverrideItems": True},
    }
    status, headers, _ = req("POST", f"/workspaces/{ws}/git/updateFromGit", body)
    if status == 202:
        wait_lro(headers, "updateFromGit")
    print("  updated to", remote[:7])


def _run_job(ws: str, name: str, item_type: str, job_type: str, execution_data: dict) -> dict:
    _, _, items = req("GET", f"/workspaces/{ws}/items?type={item_type}")
    item = next(i["id"] for i in items["value"] if i["displayName"] == name)
    _, headers, _ = req("POST", f"/workspaces/{ws}/items/{item}/jobs/instances?jobType={job_type}",
                        {"executionData": execution_data})  # fmt: skip
    started = time.time()
    while True:
        time.sleep(20)
        _, _, job = req("GET", headers["Location"])
        print(f"  {name}: {job['status']} ({int(time.time() - started)}s)", flush=True)
        if job["status"] in TERMINAL_JOB_STATES:
            if job["status"] != "Completed":
                raise SystemExit(json.dumps(job.get("failureReason")))
            return job


def run_notebook(ws: str, name: str, params: dict[str, str]) -> dict:
    parameters = {k: {"value": v, "type": "string"} for k, v in params.items()}
    return _run_job(ws, name, "Notebook", "RunNotebook", {"parameters": parameters})


def run_pipeline(ws: str, name: str, params: dict[str, str]) -> dict:
    """Run a Data Pipeline (pipeline parameters are passed as plain values) and wait for it."""
    return _run_job(ws, name, "DataPipeline", "Pipeline", {"parameters": params})


def set_git_credentials(ws: str, connection_id: str) -> None:
    """Point the caller's Git credentials for ``ws`` at a shared Fabric connection (required for service principals)."""
    body = {"source": "ConfiguredConnection", "connectionId": connection_id}
    _, _, current = req("GET", f"/workspaces/{ws}/git/myGitCredentials")
    if current and current.get("connectionId") == connection_id:
        print("  git credentials already use the configured connection")
        return
    req("PATCH", f"/workspaces/{ws}/git/myGitCredentials", body)
    print(f"  git credentials -> connection {connection_id}")


def deploy(pipeline_id: str, source_stage: str, target_stage: str, note: str) -> dict:
    """Deploy all supported items from one deployment-pipeline stage to the next (stages by display name)."""
    _, _, stages = req("GET", f"/deploymentPipelines/{pipeline_id}/stages")
    ids = {s["displayName"]: s["id"] for s in stages["value"]}
    body = {"sourceStageId": ids[source_stage], "targetStageId": ids[target_stage], "note": note[:1024]}
    status, headers, result = req("POST", f"/deploymentPipelines/{pipeline_id}/deploy", body)
    if status == 202:
        result = wait_lro(headers, f"deploy {source_stage} -> {target_stage}")
    print(f"  deployed {source_stage} -> {target_stage}")
    return result


def set_default_environment(ws: str, name: str) -> None:
    """Make ``name`` the workspace default Spark environment (notebooks rely on it; see CLAUDE.md)."""
    _, _, current = req("GET", f"/workspaces/{ws}/spark/settings")
    if (current or {}).get("environment", {}).get("name") == name:
        print(f"  default environment already {name}")
        return
    req("PATCH", f"/workspaces/{ws}/spark/settings", {"environment": {"name": name}})
    print(f"  default environment -> {name}")


if __name__ == "__main__":
    command, workspace = sys.argv[1], sys.argv[2]
    if command == "update":
        update_from_git(workspace, sys.argv[3] if len(sys.argv) > 3 else None)
    elif command == "run":
        run_notebook(workspace, sys.argv[3], json.loads(sys.argv[4]) if len(sys.argv) > 4 else {})
    elif command == "run-pipeline":
        run_pipeline(workspace, sys.argv[3], json.loads(sys.argv[4]) if len(sys.argv) > 4 else {})
    elif command == "git-credentials":
        set_git_credentials(workspace, sys.argv[3])
    elif command == "default-env":
        set_default_environment(workspace, sys.argv[3])
    elif command == "deploy":  # here `workspace` is the deployment pipeline id
        deploy(workspace, sys.argv[3], sys.argv[4], sys.argv[5] if len(sys.argv) > 5 else "")
    else:
        raise SystemExit(__doc__)
