# ADR 0003 — Transformation code as a tested package, shipped as a wheel inside the Fabric environment

- **Status:** Accepted
- **Date:** 2026-10-21

## Context

Notebooks are hard to unit-test, review and reuse. The transformations (parsing, quality rules, deduplication, keys,
aggregates, data-quality checks) are the part most likely to be wrong, so they need tests with real Spark + Delta.
Every workspace (dev, test, prod, feature workspaces) must run exactly the code that was reviewed.

## Options

| | Option | For | Against |
|---|---|---|---|
| A | **Logic inside notebooks** (shared via `%run`) | Nothing to build | No unit tests, no linting, large diffs in notebook JSON, copy-paste between notebooks |
| B | **Package on a feed** (PyPI or private), installed by the environment from a version pin | Standard Python release flow | Needs a feed and credentials, which this project avoids. Fabric resolves the package at publish time, so what runs depends on the feed, not on Git. |
| C | **`%pip install` from Git at notebook start** | No artifact to store | Slow per session, needs network and tokens, and the commit used is decided at run time |
| D | **Package in `src/`, wheel committed inside `fabric/env_divvy.Environment/Libraries/CustomLibraries/`** | Git holds the exact bytes every workspace runs. Deployment pipelines promote the environment with its library. No feed or credentials. | A binary in Git. Every `src/` change needs a version bump + wheel rebuild. Environments are only *staged* by Git and must be published. |

## Decision

**Option D.** Notebooks are thin wrappers (parameters, I/O, logging). All logic lives in `divvy_medallion` and is
tested locally with Spark + Delta (`tests/`). `tools/sync_environment_wheel.py` builds the wheel with a pinned
`hatchling`, so builds are byte-reproducible. CI rebuilds it and fails if the committed wheel differs from the
committed source (`environment wheel up to date`).

## Consequences

- Changing `src/` means: bump `pyproject.toml`, run `python tools/sync_environment_wheel.py`, commit the new wheel.
  CI enforces it.
- After Git sync, Branch out or a deployment, `env_divvy` must be published
  (`tools/publish_environment_library.py`). The deploy workflow does this at every stage. Publishing takes a few
  minutes.
- The package declares no runtime dependencies: PySpark comes from Fabric Runtime 1.3.
- **Revisit when:** several repositories share the package. Then publish it to a feed (B) and pin it in the environment.
