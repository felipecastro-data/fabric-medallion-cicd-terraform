"""Keep the wheel committed in the Fabric Environment in sync with ``src/``.

The Environment item (``fabric/env_divvy.Environment``) carries the ``divvy_medallion`` wheel in Git, so
branch-out workspaces, "Update from Git" and the deployment pipeline all get the exact library version.
Wheels are byte-reproducible (hatchling, pinned in ``pyproject.toml``), so CI can prove the committed
wheel was built from the committed source.

    python tools/sync_environment_wheel.py          # rebuild and copy the wheel into the Environment
    python tools/sync_environment_wheel.py --check  # CI: fail if the committed wheel is stale
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIBRARIES = ROOT / "fabric" / "env_divvy.Environment" / "Libraries" / "CustomLibraries"
DISTRIBUTION = "divvy_medallion"


def build_wheel(out_dir: Path) -> Path:
    subprocess.run(["uv", "build", "--wheel", "--quiet", "--out-dir", str(out_dir)], cwd=ROOT, check=True)
    (wheel,) = out_dir.glob(f"{DISTRIBUTION}-*.whl")
    return wheel


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="verify only; do not modify files")
    args = parser.parse_args(argv)

    with tempfile.TemporaryDirectory() as tmp:
        built = build_wheel(Path(tmp))
        committed = sorted(LIBRARIES.glob(f"{DISTRIBUTION}-*.whl")) if LIBRARIES.is_dir() else []

        if args.check:
            if [p.name for p in committed] != [built.name]:
                print(f"::error::Environment wheels {[p.name for p in committed]} != expected [{built.name}]")
                return 1
            if sha256(committed[0]) != sha256(built):
                print(f"::error::{built.name} in the Environment is stale; run tools/sync_environment_wheel.py")
                return 1
            print(f"{built.name} matches src/ (sha256 {sha256(built)[:12]})")
            return 0

        LIBRARIES.mkdir(parents=True, exist_ok=True)
        for old in committed:
            old.unlink()
        shutil.copy2(built, LIBRARIES / built.name)
        print(f"Synced {built.name} into {LIBRARIES.relative_to(ROOT)}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
