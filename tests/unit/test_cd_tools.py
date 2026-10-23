"""CD tooling behaviour that protects deployments (no network: Fabric/Entra/GitHub calls are faked)."""

import io
import json
import urllib.parse

import fabric_auth
import fabric_ops
import publish_environment_library as publish
import pytest


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_ci_tokens_use_a_fresh_github_oidc_assertion_for_every_renewal(monkeypatch):
    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_URL", "https://gh.example/token?x=1")
    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "gh-request-token")
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client")
    assertions, clock = iter(["oidc-1", "oidc-2"]), [1000.0]
    seen = []

    def fake_urlopen(request, timeout):
        if request.full_url.startswith("https://gh.example"):
            assert "audience=api%3A%2F%2FAzureADTokenExchange" in request.full_url
            return _Response(json.dumps({"value": next(assertions)}).encode())
        form = urllib.parse.parse_qs(request.data.decode())
        seen.append(form["client_assertion"][0])
        assert form["grant_type"] == ["client_credentials"] and form["client_id"] == ["client"]
        return _Response(json.dumps({"access_token": f"fabric-{len(seen)}"}).encode())

    monkeypatch.setattr(fabric_auth.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(fabric_auth.time, "time", lambda: clock[0])
    monkeypatch.setattr(fabric_auth, "_cache", {})

    assert fabric_auth.access_token() == "fabric-1"
    assert fabric_auth.access_token() == "fabric-1"  # cached
    clock[0] += fabric_auth.RENEW_AFTER_S + 1
    assert fabric_auth.access_token() == "fabric-2"
    assert seen == ["oidc-1", "oidc-2"]  # a new GitHub assertion per renewal, never the expired first one


def _git_status(remote, head=None, changes=()):
    return {"remoteCommitHash": remote, "workspaceHead": head or remote, "changes": list(changes)}


def test_update_from_git_refuses_a_branch_head_other_than_the_runs_commit(monkeypatch):
    calls = []
    monkeypatch.setattr(
        fabric_ops, "req", lambda m, u, b=None: (calls.append((m, u)), (200, {}, _git_status("new")))[1]
    )

    with pytest.raises(SystemExit, match="not this run's commit"):
        fabric_ops.update_from_git("ws", expected_commit="old")
    assert [m for m, _ in calls] == ["GET"]  # nothing was synced


def test_update_from_git_applies_the_expected_commit(monkeypatch):
    calls = []

    def fake_req(method, url, body=None):
        calls.append((method, url, body))
        return (200, {}, _git_status("abc", head="old")) if method == "GET" else (200, {}, None)

    monkeypatch.setattr(fabric_ops, "req", fake_req)
    fabric_ops.update_from_git("ws", expected_commit="abc")

    post = calls[-1]
    assert post[0] == "POST" and post[1].endswith("/git/updateFromGit")
    assert post[2]["remoteCommitHash"] == "abc"


class _FakeEnv:
    """Environment whose GET returns a scripted sequence of publishDetails."""

    def __init__(self, details):
        self.details = iter(details)

    def request(self, method, path, body=None, content_type=None):
        return 200, {}, {"properties": {"publishDetails": next(self.details)}}


def test_publish_wait_ignores_the_previous_publish_state(monkeypatch):
    monkeypatch.setattr(publish.time, "sleep", lambda s: None)
    api = _FakeEnv(
        [
            {"targetVersion": "v1", "state": "Success"},  # still the old publish
            {"targetVersion": "v1", "state": "Failed"},  # ... even a stale failure must not count
            {"targetVersion": "v2", "state": "Running"},
            {"targetVersion": "v2", "state": "Success"},
        ]
    )
    assert publish.wait_for_publish(api, "ws", "env", 600, previous_version="v1", target_version=None) == "success"


def test_publish_wait_follows_the_returned_target_version(monkeypatch):
    monkeypatch.setattr(publish.time, "sleep", lambda s: None)
    api = _FakeEnv([{"targetVersion": "v2", "state": "Success"}, {"targetVersion": "v3", "state": "Failed"}])
    assert publish.wait_for_publish(api, "ws", "env", 600, previous_version="v1", target_version="v3") == "failed"
