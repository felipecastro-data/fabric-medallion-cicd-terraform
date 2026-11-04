"""Data contracts: versioned YAML files shipped inside the package and enforced by the data-quality suite."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources

SILVER_TRIPS = "silver_trips.yml"


@dataclass(frozen=True)
class Column:
    name: str
    type: str  # Spark simpleString: string, int, double, boolean, date, timestamp
    required: bool
    accepted_values: tuple[str, ...] = ()


@dataclass(frozen=True)
class DataContract:
    dataset: str
    version: str
    owner: str
    primary_key: tuple[str, ...]
    partitioned_by: str
    max_lag_months: int
    freshness_severity: str
    columns: tuple[Column, ...]

    @property
    def required(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns if c.required)


def parse(document: dict) -> DataContract:
    columns = tuple(
        Column(c["name"], c["type"], bool(c.get("required", False)), tuple(c.get("accepted_values", ())))
        for c in document["schema"]
    )
    return DataContract(
        dataset=document["dataset"],
        version=str(document["version"]),
        owner=document["owner"],
        primary_key=tuple(document["primary_key"]),
        partitioned_by=document["partitioned_by"],
        max_lag_months=int(document["freshness"]["max_lag_months"]),
        freshness_severity=document["freshness"].get("severity", "warn"),
        columns=columns,
    )


def load(name: str = SILVER_TRIPS) -> DataContract:
    """Load a contract shipped with the package (PyYAML is part of Fabric Runtime 1.3)."""
    import yaml

    text = resources.files(__package__).joinpath(name).read_text(encoding="utf-8")
    return parse(yaml.safe_load(text))
