"""Environment-agnostic lakehouse paths.

Notebooks resolve lakehouses by *name* in the current workspace, so the same code runs unchanged in
dev, test, prod and feature workspaces (names are identical everywhere; IDs are not).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Lakehouse:
    name: str
    abfs_path: str
    default_schema: str = "dbo"

    def table(self, table: str, schema: str | None = None) -> str:
        return f"{self.abfs_path}/Tables/{schema or self.default_schema}/{table}"

    def files(self, *parts: str) -> str:
        return "/".join([f"{self.abfs_path}/Files", *(p.strip("/") for p in parts)])


def resolve(name: str) -> Lakehouse:
    """Resolve a lakehouse in the current Fabric workspace (only available inside Fabric)."""
    import notebookutils  # type: ignore[import-not-found]  # provided by the Fabric runtime

    properties = notebookutils.lakehouse.getWithProperties(name)["properties"]
    return Lakehouse(name=name, abfs_path=properties["abfsPath"])
