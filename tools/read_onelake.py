"""Read a lakehouse Delta table over OneLake with delta-rs, as the signed-in Azure CLI user (verification only).

    uv run --no-project --with deltalake --with pandas --with pyarrow \\
        python tools/read_onelake.py <workspace-id> <lakehouse> <table> ["<python statement over dt>"]

Without a statement it prints the last 10 rows; e.g. ``"print(dt.to_pandas().groupby('_period').size())"``.
"""

from __future__ import annotations

import json
import subprocess
import sys
import urllib.request

from deltalake import DeltaTable


def _token(resource: str) -> str:
    cmd = ["az", "account", "get-access-token", "--resource", resource, "--query", "accessToken", "-o", "tsv"]
    return subprocess.check_output(cmd, text=True).strip()


def open_table(ws: str, lakehouse: str, table: str) -> DeltaTable:
    request = urllib.request.Request(
        f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/lakehouses",
        headers={"Authorization": f"Bearer {_token('https://api.fabric.microsoft.com')}"},
    )
    with urllib.request.urlopen(request) as response:
        lakehouse_id = next(x["id"] for x in json.load(response)["value"] if x["displayName"] == lakehouse)
    uri = f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/dbo/{table}"
    storage = {"bearer_token": _token("https://storage.azure.com"), "use_fabric_endpoint": "true"}
    return DeltaTable(uri, storage_options=storage)


if __name__ == "__main__":
    dt = open_table(*sys.argv[1:4])
    if len(sys.argv) > 4:
        exec(sys.argv[4])  # ad-hoc inspection statement over `dt` (local verification tool)
    else:
        print(dt.to_pandas().tail(10).to_string())
