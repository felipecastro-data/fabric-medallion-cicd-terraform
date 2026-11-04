# ADR 0004 — `dim_station` keeps history (SCD2), detected by diffing Silver snapshots

- **Status:** Accepted (supersedes the type 1 full rebuild of `dim_station`)
- **Date:** 2026-11-04

## Context

Divvy stations get renamed ("Clark & Elm" → "Clark St & Elm St") and occasionally move. Until v0.4.0,
`dim_station` was rebuilt in full from Silver `stations` on every change. That overwrote the old name, so a report
on last year's trips showed this year's names, and nobody could tell when a station changed.

Silver `stations` is a **snapshot**: one row per `station_id` with its most frequent name, median coordinates and
first/last day seen, recomputed from all loaded trips. The source has no change feed for stations.

Facts must not need re-keying when a station changes. `fact_trips` holds 6M rows, and re-keying would mean a full
rebuild.

## Options

| | Option | For | Against |
|---|---|---|---|
| A | **Type 1 full rebuild** (previous) | Simplest; always current | History lost; changes invisible |
| B | **SCD2 with a version key in the fact** (`fact_trips.start_station_version_key`) | Classic star join, no date predicate | The fact must be re-keyed or rebuilt when a version opens, and Gold loads per period would need the dimension first. A breaking change to `fact_trips`. |
| C | **SCD2 with a durable key in the fact, effective dates in the dimension** | Facts unchanged (`station_key` is already a deterministic hash of `station_id`). "Current name" is a plain join; "name at the time" is an as-of join on `valid_from`/`valid_to`. | As-of joins need a range predicate. Version boundaries are **load time**, not the instant Divvy renamed the station. |
| D | **SCD2 dated from trip events** (date of the first trip carrying the new name) | Accurate effective dates | Names in the raw data flip back and forth for weeks during a rename (both names appear). A modal name per day would create noisy versions, and it is much more expensive to compute. |

## Decision

**Option C**, implemented in `divvy_medallion.gold.dimensions` and tested in `tests/unit/test_dim_station_scd2.py`:

- Change detection is a **snapshot diff** (CDC without a change feed). Each Gold run compares the Silver snapshot
  with the current versions:
  - **new station** → a version valid since 1900-01-01, so every trip resolves;
  - **renamed**, or **relocated more than 100 m** → close the current version at the run time and open a new one;
  - **coordinate drift under 100 m, first/last seen** → type 1 update of the current version. Medians shift slightly
    as months are added, and that is not a real change.
  - Stations missing from the snapshot keep their current version: history is never deleted.
- One Delta **MERGE** applies all changes atomically (staged rows: `close`, `type1`, `insert`).
- Columns: `station_version_key` (hash of `station_id` + `valid_from`), durable `station_key`, attributes,
  `valid_from`, `valid_to` (open = 9999-12-31), `is_current`, `change_reason`
  (`initial`, `new`, `renamed`, `relocated`, `renamed+relocated`).
- The data-quality gate checks the SCD2 invariants on every run: unique version key, exactly one current version
  per station, contiguous intervals (no gaps or overlaps). Fact foreign keys are checked against current versions.

## Consequences

- `fact_trips` and the aggregates are unchanged: no rebuild and no re-keying.
- As-of query:

  ```sql
  SELECT f.ride_id, s.station_name
  FROM fact_trips f
  JOIN dim_station s
    ON s.station_key = f.start_station_key
   AND f.started_at >= s.valid_from AND f.started_at < s.valid_to
  ```

- The first run on an existing workspace converts the type 1 table into initial versions. It held no history, so
  nothing is lost, and the run is logged in `processing_log` like any other `gold.dim_station` build.
- A version starts when the pipeline *noticed* the change: up to a week after it happened with the weekly schedule.
- **Revisit when:** Divvy publishes a station feed with change dates (GBFS `station_information` snapshots). Then
  date versions from the feed (option D's accuracy without its noise).
