# Design: scalability, cost, governance and security

How the platform behaves today, what it costs, who owns and can reach what, and what changes as it grows. The
reasoning behind the main choices is in the [architecture decision records](adr/).

Figures are from the production workspace on 2026-11-04 (12 months loaded, 202510–202609), unless stated otherwise.

## 1. Scalability

### Current volumes

| Layer | Table | Rows | Size (Delta) | Partitioning |
|---|---|---:|---:|---|
| Bronze | `divvy_trips` | 6,171,785 | 315 MB | `_period` (12) |
| Silver | `trips` / `trips_quarantine` | 6,006,901 / 164,884 | — | `_period` |
| Gold | `fact_trips` | 6,006,901 | 242 MB | `_period` |
| Gold | `dim_station` | 2,028 | small | — |

The busiest month (July 2026) is about 870k trips. Raw `.zip`/`.csv` files are also archived under
`lh_bronze/Files/raw/divvy/<period>/` so any month can be replayed.

### Compute sizing per environment (Terraform)

| Environment | Starter pool max nodes | Max executors | Session timeout |
|---|---:|---:|---:|
| dev | 2 | 1 | 20 min |
| test | 2 | 1 | 20 min |
| prod | 4 | 3 | 30 min |

`env_divvy` asks for a 4-core / 28 GB driver and 1–3 executors of the same size (dynamic allocation). A CI test proves
the environment fits the smallest Terraform pool. That test exists because a deploy to test once failed when the
environment asked for 80 cores on a 16-core pool.

### Measured runtimes (prod, from the Fabric run history)

| Run | Work | Total | Bronze trips | Silver trips | Gold | DQ |
|---|---|---:|---:|---:|---:|---:|
| 2026-10-23 | first load, 2 months | 26 min | 4.5 min | 10.2 min | 5.2 min | 5.8 min |
| 2026-10-26 (scheduled) | backfill, 10 months | 47 min | 7.9 min | 21.8 min | 9.2 min | 7.7 min |
| 2026-10-23 | nothing new (re-run) | 18 min | 4.0 min | 4.2 min | 3.7 min | 5.2 min |

The no-op run shows the fixed cost: **about 4 minutes per notebook is Spark session start-up**. Bronze weather and
Bronze trips run in parallel, and the rest is sequential. Actual work scales roughly linearly with months: Silver trips
takes about 1.5–2 minutes per month, including deduplication across the month.

### Growth model

- **Partition per period.** Every write replaces one `_period` partition, so a run's cost depends on the months that
  changed, not on table size. Twelve more months add 12 partitions and no extra work per run.
- **Dimensions** are small (about 2k stations, about 2k dates). `dim_date` and `dim_weather` are rebuilt in full.
  `dim_station` is merged (SCD2), so it grows by one row per real rename or relocation, not per run.
- **Aggregates** are rebuilt per trip month from at most two source files (Divvy's files overlap at month boundaries).

### What changes at 10× (about 60M trips a year, or about 9M per month)

| Pressure | Change |
|---|---|
| Silver dedup per month needs more memory | Prod pool to medium/large nodes and more executors (Terraform variables only). Enable the native execution engine. |
| One partition per month is too coarse | Partition by `trip_date` (or month + `ZORDER BY start_station_key`) and run `OPTIMIZE` after each load |
| Session start-up dominates small runs | Run Bronze → Gold in one high-concurrency session, or use a custom pool with warm nodes |
| Month-grain watermarks re-process too much | Switch Silver → Gold to Delta Change Data Feed (see [ADR 0002](adr/0002-control-table-watermarks-vs-delta-cdf.md)) |
| DQ checks recount whole tables | Restrict checks to the periods processed in the run (already done for period checks) and keep table checks on samples |

## 2. Cost

### Capacity

All three workspaces run on one **Fabric trial capacity (FTL64, West US)**: 64 capacity units (CU), free until the
trial ends (about 2026-12-11). Fabric bills a capacity per hour while it is running, whatever the load. Spark draws
**2 Spark vCores per CU**, with bursting.

### Estimated capacity units per `pl_medallion` run

The exact figure comes from the *Fabric Capacity Metrics* app (not installed in this tenant). The estimate below
uses the measured notebook time and the session size (driver + up to 3 executors, 4 vCores each = up to 16 vCores =
**up to 8 CU** per session):

| Run type | Notebook-minutes | CU-hours (upper bound) | Pay-as-you-go equivalent* |
|---|---:|---:|---:|
| Weekly run, nothing new (2026-10-23 re-run) | 26 | 3.4 | about $0.60 |
| Two new months (2026-10-23 first load) | 34 | 4.5 | about $0.80 |
| 10-month backfill (2026-10-26, includes one retried weather attempt) | 64 | 8.5 | about $1.55 |

\* At about **$0.18 per CU-hour** (F-SKU pay-as-you-go list price in US regions; check your region). Dynamic allocation
usually keeps sessions at 1 executor, so real usage is lower.

### What a paid deployment would cost

| Choice | Monthly cost (approx.) | Notes |
|---|---:|---|
| F8 always on | about $1,050 | Fits two parallel 16-vCore sessions with bursting. Overkill for one weekly run. |
| F8 resumed for the weekly run only (about 1 h a week) | about $6 + storage | Pause and resume via the Azure API from the workflow. The capacity must be Active for `terraform apply`. |
| OneLake storage (about 1 GB of Delta + raw files) | < $0.10 | Negligible |

The main cost lever is **capacity uptime**, not data volume. Then comes session start-up: about 4 minutes × 6
notebooks per run.

### Cost of the schedule

The weekly prod run is incremental, and a run with nothing new still costs the session start-ups (about 26 notebook-minutes,
about 3.4 CU-hours). A daily schedule would cost about 7× that for at most one new month. That is why the schedule is
weekly. The schedule and the daily monitor are paused since 2026-11-04 because the project is complete.

## 3. Governance

### Ownership and layers

| Layer | Lakehouse | Contents | Contract | Owner |
|---|---|---|---|---|
| Bronze | `lh_bronze` | Raw Divvy trips (all strings) + raw files, Open-Meteo JSON, `ingestion_log` | Divvy header check (schema drift fails the period) | Platform / data engineering |
| Silver | `lh_silver` | Typed, deduplicated `trips`, `trips_quarantine` with reason codes, `stations`, `weather_daily`, `processing_log` | Quality rules in `silver/trips.py` + data contract [`silver_trips.yml`](../src/divvy_medallion/contracts/silver_trips.yml) (schema, required columns, key, accepted values, owner, freshness SLA), enforced every run | Data engineering |
| Gold | `lh_gold` | Star schema (`dim_station` with SCD2 history), aggregates, `dq_results`, `processing_log` | 58 data-quality checks for a two-month run (incl. SCD2 invariants), `error` checks fail the pipeline | Data engineering (consumers read Gold only) |

### Lineage

Every Bronze row carries `_batch_id`, `_period`, `_source_file` and `_ingested_at`. Silver keeps them, and
`fact_trips._bronze_batch_id` points back to the Bronze load. Each control-table row links a period to the upstream
batch it consumed, so any Gold partition can be traced to the exact source file and run.

### Retention (current state and policy)

| Data | Current | Policy |
|---|---|---|
| Raw files (`Files/raw/…`) | Kept indefinitely | Keep: they make every month replayable |
| Bronze / Silver / Gold Delta history | Default Delta retention (7 days for `VACUUM`) | `VACUUM` is not scheduled. Add it with `OPTIMIZE` when tables grow. |
| `trips_quarantine` | Kept with the period it belongs to | Keep 13 months, like the trips it explains (it is replaced when a period is re-loaded) |
| `ingestion_log`, `processing_log`, `dq_results` | Append-only | Keep: they are the audit trail and the monitor's input |

Divvy data is published under the Lyft Bikes and Scooters data license and contains no personal data: rides are
anonymous, with no rider IDs.

## 4. Security

### Identities

| Identity | Used by | Access |
|---|---|---|
| Entra service principal (app `b4f0cf77-…`) | GitHub Actions via **OIDC federation** (no client secret) | Creates the workspaces through Terraform, so it is Admin on dev/test/prod and the deployment pipeline. Fabric API scope only. |
| Human administrator | Portal work, approvals | Admin on every workspace and the deployment pipeline (Terraform `admin_user_object_id`) |
| Consumers | Read Gold | Not granted in this repo. The mesh project reads `lh_gold` through OneLake shortcuts with per-role security. |

Federated credentials are scoped to this repository's `main` branch, pull requests and the `platform` / `dev` /
`test` / `prod` environments. The subjects use GitHub's immutable owner and repository IDs, so a renamed or re-created
repository with the same name cannot get a token, and neither can a workflow from a fork. The only stored
secret is `TF_API_TOKEN` (HCP Terraform state).

### Who can change what

| Change | Gate |
|---|---|
| Anything on `main` | Pull request, green `ci`, all conversations resolved, squash merge (`protect-main`, no bypass) |
| Platform (Terraform apply) | GitHub environment `platform`: required reviewer `felipecastro-data` |
| Production deploy | GitHub environment `prod`: required reviewer `felipecastro-data`, after dev and test ran `pl_medallion` green |
| Prod data | Only `pl_medallion` writes it. Lakehouses have `prevent_destroy`, and breaking schema changes require an explicit `rebuild=True`. |

### Hardening for a real tenant

- Split the service principal: one per stage (dev/test vs prod) with **Contributor** instead of Admin, and keep
  workspace creation in a separate platform identity.
- Use `prevent_self_review: true` on `prod` and `platform` once there is more than one maintainer.
- Grant consumers **Viewer** on Gold only (or OneLake data access roles per table), never on Bronze/Silver.
- Turn on workspace outbound access protection and private links if the tenant needs network isolation. Today Bronze
  downloads from two public HTTPS endpoints.
