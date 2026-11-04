# ADR 0002 — Control-table watermarks per period instead of Delta Change Data Feed

- **Status:** Accepted
- **Date:** 2026-10-21

## Context

Divvy publishes one file per month, and a month can be re-published or re-loaded (`force=True`). Every layer must be
**incremental** (only rework what changed upstream), **idempotent** (a re-run is a no-op) and **repairable** (a
failed step is fixed by a plain retry). The natural unit of change is the **period** (`YYYYMM`). Bronze replaces a
whole partition (`replaceWhere _period = …`) rather than upserting rows.

We also need an audit trail: for each source and period, which batch was loaded, with how many rows, and why it
failed.

## Options

| | Option | For | Against |
|---|---|---|---|
| A | **Delta Change Data Feed**: each consumer stores the last table version it read and pulls `table_changes` | Row-level, standard Delta feature | A partition overwrite surfaces as a full delete + insert of the month, so there is no saving at our grain. Each consumer must persist versions per upstream table. Replays are limited by `VACUUM` retention. CDF must be enabled before the first write. |
| B | **Structured Streaming with checkpoints** (`trigger(availableNow=True)`) | Exactly-once bookkeeping for free | Checkpoints are opaque and hard to reset for one period. A schema change invalidates them. Partition overwrites in the source break append-only streaming reads. |
| C | **Timestamp high-water mark** (`max(_ingested_at)`) | Trivial | Misses a re-load of an *older* period. One number per table cannot say which periods failed. |
| D | **Control tables** `ingestion_log` / `processing_log`: one row per `(source, period, batch_id, status)` | The upstream watermark is "latest successful `batch_id` per period". A downstream step processes a period when it has not yet processed that batch. The same table is the audit log and the monitor's input. | We maintain the bookkeeping ourselves, and it must be tested. Granularity is a whole month. |

## Decision

**Option D**, implemented in `src/divvy_medallion/control.py` and covered by unit tests:

- `latest_succeeded_batches` gives the upstream watermark per period. `processed_batches` + `periods_to_process` pick
  the work.
- `combine_watermarks` builds a composite watermark for steps with several inputs. Gold re-runs a month when *either*
  its trips or its weather got a newer batch.
- `record_rebuild` writes a generation marker *before* tables are dropped, so pre-rebuild checkpoints stop counting.
  A period that fails mid-rebuild is retried instead of skipped.
- A failed period is logged as `FAILED`, and the next run picks it up because it never succeeded. A month Divvy has
  not published yet is logged as `NOT_PUBLISHED` (#16).

## Consequences

- Re-processing granularity is a month (130k–870k rows). That takes seconds to minutes on a starter pool.
- The logs are also the operational record: `tools/monitor_prod.py` and the README results are computed from them.
- **Revisit when:** sources arrive daily or as row-level changes (CDC from an OLTP system), or a month grows past tens
  of millions of rows. Then switch Silver → Gold to CDF (A) or streaming (B), keeping the control table as the audit log.
