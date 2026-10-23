"""Read a lakehouse Delta table over OneLake with delta-rs (Azure CLI user locally, service principal in CI).

    uv run --no-project --with deltalake --with pandas --with pyarrow \\
        python tools/read_onelake.py <workspace-id> <lakehouse> <table> ["<python statement over dt>"]

Without a statement it prints the last 10 rows; e.g. ``"print(dt.to_pandas().groupby('_period').size())"``.
"""

from __future__ import annotations

import json
import sys
import urllib.request

from deltalake import DeltaTable
from fabric_auth import access_token  # fresh GitHub OIDC exchange in CI, az CLI locally

STORAGE_SCOPE = "https://storage.azure.com/.default"


def open_table(ws: str, lakehouse: str, table: str) -> DeltaTable:
    request = urllib.request.Request(
        f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/lakehouses",
        headers={"Authorization": f"Bearer {access_token()}"},
    )
    with urllib.request.urlopen(request) as response:
        lakehouse_id = next(x["id"] for x in json.load(response)["value"] if x["displayName"] == lakehouse)
    uri = f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/dbo/{table}"
    storage = {"bearer_token": access_token(STORAGE_SCOPE), "use_fabric_endpoint": "true"}
    return DeltaTable(uri, storage_options=storage)


if __name__ == "__main__":
    dt = open_table(*sys.argv[1:4])
    if len(sys.argv) > 4:
        exec(sys.argv[4])  # ad-hoc inspection statement over `dt` (local verification tool)
    else:
        print(dt.to_pandas().tail(10).to_string())
