"""Publish a Fabric Environment, optionally uploading a Python wheel first.

Fabric's "Update from Git" and "Branch out" only *stage* an Environment (libraries + Spark compute); the
active version stays the previous publish until someone publishes. Run without a wheel to publish what
Git staged; with a wheel to replace the staged wheel of the same distribution first. Waits for the publish
to finish. Authenticates with the Azure CLI (``az login`` locally, ``azure/login`` OIDC in CI).

Usage:
    python tools/publish_environment_library.py --workspace-id <id>                 # publish staged
    python tools/publish_environment_library.py --workspace-id <id> dist/x.whl      # upload + publish
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from fabric_auth import access_token  # fresh GitHub OIDC exchange in CI, az CLI locally

API = "https://api.fabric.microsoft.com/v1"
TERMINAL_STATES = {"success", "failed", "cancelled"}


class Fabric:
    def request(self, method: str, path: str, body: bytes | None = None, content_type: str | None = None):
        url = path if path.startswith("http") else f"{API}{path}"
        headers = {"Authorization": f"Bearer {access_token()}"}
        if content_type:
            headers["Content-Type"] = content_type
        for attempt in range(6):
            req = urllib.request.Request(url, data=body, method=method, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=300) as response:  # noqa: S310 - fixed https host
                    raw = response.read()
                    return response.status, dict(response.headers), json.loads(raw) if raw else None
            except urllib.error.HTTPError as err:
                if err.code == 429 and attempt < 5:
                    time.sleep(int(err.headers.get("Retry-After", "10")))
                    continue
                detail = err.read().decode(errors="replace")
                raise RuntimeError(f"{method} {url} -> HTTP {err.code}: {detail}") from err
        raise RuntimeError(f"{method} {url}: retries exhausted")


def find_environment(api: Fabric, workspace_id: str, name: str) -> str:
    _, _, payload = api.request("GET", f"/workspaces/{workspace_id}/environments")
    for item in payload.get("value", []):
        if item["displayName"] == name:
            return item["id"]
    raise SystemExit(f"Environment '{name}' not found in workspace {workspace_id}")


def staged_wheels(api: Fabric, base: str) -> list[str]:
    try:
        _, _, payload = api.request("GET", f"{base}/staging/libraries")
    except RuntimeError as err:
        if "EnvironmentLibrariesNotFound" in str(err):  # empty environment answers 404, not []
            return []
        raise
    payload = payload or {}
    names = [lib.get("name") for lib in payload.get("libraries", []) if str(lib.get("name", "")).endswith(".whl")]
    names += (payload.get("customLibraries") or {}).get("wheelFiles") or []  # legacy response shape
    return sorted(set(filter(None, names)))


def publish_details(api: Fabric, workspace_id: str, environment_id: str) -> dict:
    _, _, env = api.request("GET", f"/workspaces/{workspace_id}/environments/{environment_id}")
    return (env.get("properties") or {}).get("publishDetails") or {}


def wait_for_publish(
    api: Fabric, workspace_id: str, environment_id: str, timeout_s: int, previous_version: str | None,
    target_version: str | None,
) -> str:  # fmt: skip
    """Wait for *this* publish and ignore the state of the one before it.

    A state only counts once ``publishDetails`` shows our ``targetVersion`` (when the publish request returned it)
    or, failing that, any target version other than the one recorded *before* the request.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        details = publish_details(api, workspace_id, environment_id)
        version = details.get("targetVersion")
        ours = version == target_version if target_version else bool(version) and version != previous_version
        state = str(details.get("state", "unknown")).lower() if ours else "waiting for this publish"
        print(f"  publish state: {state}")
        if ours and state in TERMINAL_STATES:
            return state
        time.sleep(30)
    raise TimeoutError(f"Environment publish did not finish within {timeout_s}s")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "wheel",
        type=Path,
        nargs="?",
        help="wheel to upload; omit to publish what is already staged ('Update from Git' only stages)",
    )
    parser.add_argument("--workspace-id", required=True)
    parser.add_argument("--environment", default="env_divvy")
    parser.add_argument("--timeout", type=int, default=1800, help="publish timeout in seconds")
    args = parser.parse_args(argv)

    api = Fabric()
    environment_id = find_environment(api, args.workspace_id, args.environment)
    base = f"/workspaces/{args.workspace_id}/environments/{environment_id}"
    print(f"Environment {args.environment} ({environment_id}); staged: {staged_wheels(api, base)}")

    wheel: Path | None = args.wheel
    if wheel is not None:
        distribution = wheel.name.split("-")[0]
        for old in staged_wheels(api, base):
            if old.split("-")[0] == distribution and old != wheel.name:
                print(f"  removing staged {old}")
                api.request("DELETE", f"{base}/staging/libraries/{urllib.parse.quote(old)}")

        print(f"  uploading {wheel.name} ({wheel.stat().st_size / 1024:.0f} KiB)")
        api.request(
            "POST",
            f"{base}/staging/libraries/{urllib.parse.quote(wheel.name)}",
            body=wheel.read_bytes(),
            content_type="application/octet-stream",
        )

    print("  publishing (this usually takes 3-10 minutes)")
    previous = publish_details(api, args.workspace_id, environment_id).get("targetVersion")
    _, _, started = api.request("POST", f"{base}/staging/publish?beta=false")
    target = ((started or {}).get("publishDetails") or {}).get("targetVersion")  # 200 response carries it
    state = wait_for_publish(api, args.workspace_id, environment_id, args.timeout, previous, target)
    if state != "success":
        print(f"::error::Environment publish finished with state '{state}'")
        return 1
    print(f"Published {args.environment}: {staged_wheels(api, base)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
