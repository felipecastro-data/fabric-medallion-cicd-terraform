# Project plan & progress

**Progress: Step 5 of 8 — 62%**

| # | Step | Status |
|---|------|--------|
| 1 | Foundations — repo, local tools, Entra app + GitHub OIDC, Fabric tenant settings, HCP Terraform | ✅ Done |
| 2 | Terraform — dev/test/prod workspaces, lakehouses, environment, roles, deployment pipeline, TF CI | ✅ Done |
| 3 | Git integration & branching — dev workspace ↔ `/fabric`, branch protection, PR template | ✅ Done |
| 4 | Bronze — idempotent incremental ingestion (Divvy + Open-Meteo), watermark, metadata | ✅ Done |
| 5 | Silver — `src/` PySpark package as wheel, cleansing, dedup, quarantine, station dim | 🟡 In progress |
| 6 | Gold & data quality — star schema, aggregates, `dq_results`, pytest + chispa | ⬜ |
| 7 | Orchestration & CD — Data Pipeline, GitHub Actions CI/CD, Deployment Pipeline, approvals, smoke tests | ⬜ |
| 8 | Observability & showcase — run log, DQ trends, failure alerts, README screenshots, LinkedIn post | ⬜ |

## Step 1 checklist

- [x] Public GitHub repo created
- [x] Local tools: Homebrew, Terraform, Azure CLI, gh, uv, Java 17
- [x] Repo skeleton committed
- [x] Entra app registration `sp-fabric-medallion-cicd` + GitHub OIDC federated credentials (main, PRs, dev/test/prod environments)
- [x] Security group `sg-fabric-automation` (scopes Fabric tenant settings to the service principal)
- [x] Fabric tenant settings for service principals + trial capacity contributor permissions
- [x] OIDC smoke test green (`fabric-auth-check` workflow: SP sees the trial capacity)
- [x] HCP Terraform organization `felipecastro-data` (execution mode: Local, state only)

## Step 2 checklist

- [x] Terraform root + reusable module `fabric_workspace_stack` (workspace, lakehouses, Spark settings, roles)
- [x] Deployment pipeline Development → Test → Production with stage ↔ workspace assignment
- [x] Local `terraform plan` against the trial capacity: 20 to add
- [x] `terraform` workflow: fmt · validate · tflint · plan comment on PR · approval-gated apply on main
- [x] PR #1 opened, plan reviewed, merged
- [x] Apply approved and green (20 resources); re-plan shows no drift
- [x] Lakehouses paired by name across all pipeline stages ("Same as source")
- [x] Workspace + pipeline IDs stored as GitHub repo variables for the CD workflow

## Step 3 checklist

- [x] `divvy-medallion-dev` connected to GitHub (`main` / `fabric/`), initial sync committed by Fabric
- [x] `ci` workflow: ruff lint + format, pytest, Fabric item validator (naming, logicalId, JSON), single `ci` gate check
- [x] PR template + CODEOWNERS
- [x] PR #2: Copilot review found 2 validator gaps → fixed with regression tests before merge (conversation resolution enforced)
- [x] `main` protected by ruleset `protect-main`: PR required (squash only), `ci` check required, no force-push / deletion, linear history
- [x] Branch-workspace flow documented in [development-workflow.md](development-workflow.md)

## Step 4 checklist

- [x] Feature workspace `divvy-medallion-feat-bronze` via Branch out (`feat/bronze-ingestion`)
- [x] `env_divvy` environment (Runtime 1.3) carrying the `divvy_medallion` wheel in Git; CI verifies wheel == `src/`
- [x] Package: periods, lakehouse-by-name, idempotent `overwrite_partition`, `ingestion_log`, Divvy + Open-Meteo readers (37 tests)
- [x] Notebooks `nb_bronze_divvy_trips`, `nb_bronze_weather_daily` (parameterised, no hard-coded IDs)
- [x] Test run 202609 in the feature workspace: 770,562 trips (= source rows), 30 weather days
- [x] PR #4 merged (Copilot review: 2 real fixes — CSV columns bound by name, env fail-fast guard; 2 outdated API claims rebutted with docs)
- [x] Dev workspace updated from Git; `env_divvy` published (Git only *stages* Environments) and set as default

## Step 5 checklist

- [x] `silver.trips`: types, trims, derived duration/calendar/distance/round-trip/dockless; 9 quality rules → `dq_reasons`
- [x] `trips_quarantine` (nothing dropped), dedup on `ride_id`, `stations` dimension (modal name, median coords), `weather_daily` (WMO conditions)
- [x] Upstream watermark: a period is rebuilt only when Bronze has a newer successful batch (`processing_log`)
- [x] Package 0.2.0, 61 tests; publish tool gains publish-only mode
- [x] Feature-workspace run 202608–202609: valid + quarantined = Bronze rows exactly; 1,697 stations
- [ ] PR merged; dev updated + `env_divvy` published
