"""Guard against test values committed from a Fabric workspace (e.g. periods = "202609")."""

import re
from pathlib import Path

import pytest

NOTEBOOKS = sorted((Path(__file__).resolve().parents[2] / "fabric").glob("*.Notebook/notebook-content.py"))
EXPECTED_DEFAULTS = {"periods": '"last:12"', "force": "False", "rebuild": "False", "batch_id": '""'}


def _parameters(source: str) -> dict[str, str]:
    cell = source.split("# PARAMETERS CELL ********************", 1)[1].split("# METADATA", 1)[0]
    return dict(re.findall(r"^(\w+)\s*=\s*(\S+)", cell, flags=re.MULTILINE))


@pytest.mark.parametrize("notebook", NOTEBOOKS, ids=lambda p: p.parent.name)
def test_parameter_defaults_are_production_values(notebook):
    params = _parameters(notebook.read_text())
    unexpected = {k: v for k, v in params.items() if k in EXPECTED_DEFAULTS and v != EXPECTED_DEFAULTS[k]}
    assert not unexpected, f"{notebook.parent.name}: non-default parameters committed: {unexpected}"
