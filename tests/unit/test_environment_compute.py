"""env_divvy's Spark compute must fit the smallest starter pool Terraform provisions.

Fabric validates this only when a session starts, so a too-large environment fails at runtime in the stage with the
smallest pool (it happened: 80 claimed cores vs a 16-core pool in test). Starter pools use Medium nodes.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPARK_COMPUTE = ROOT / "fabric" / "env_divvy.Environment" / "Setting" / "Sparkcompute.yml"
TERRAFORM_VARIABLES = ROOT / "infra" / "terraform" / "variables.tf"
MEDIUM_NODE = {"cores": 8, "memory_gb": 56}


def _compute() -> dict[str, str]:
    return dict(re.findall(r"^\s*(\w+):\s*(\S+)\s*$", SPARK_COMPUTE.read_text(), flags=re.MULTILINE))


def _smallest_pool_nodes() -> int:
    return min(int(n) for n in re.findall(r"starter_pool_max_nodes\s*=\s*(\d+)", TERRAFORM_VARIABLES.read_text()))


def test_environment_claim_fits_the_smallest_terraform_pool():
    c = _compute()
    executors = int(c["max_executors"]) if c.get("enabled", "false") == "true" else 1
    claimed_cores = int(c["driver_cores"]) + executors * int(c["executor_cores"])
    claimed_memory = int(c["driver_memory"].rstrip("g")) + executors * int(c["executor_memory"].rstrip("g"))

    nodes = _smallest_pool_nodes()
    assert claimed_cores <= nodes * MEDIUM_NODE["cores"], f"{claimed_cores} cores > pool of {nodes} Medium nodes"
    assert claimed_memory <= nodes * MEDIUM_NODE["memory_gb"], f"{claimed_memory} GB > pool of {nodes} Medium nodes"


def test_environment_runtime_matches_the_tested_spark_version():
    assert _compute()["runtime_version"] == "1.3"  # Spark 3.5 / Python 3.11, what CI tests against
