# Project plan & progress

**Progress: Step 3 of 8 — 30%**

| # | Step | Status |
|---|------|--------|
| 1 | Foundations — repo, local tools, Entra app + GitHub OIDC, Fabric tenant settings, HCP Terraform | ✅ Done |
| 2 | Terraform — dev/test/prod workspaces, lakehouses, environment, roles, deployment pipeline, TF CI | ✅ Done |
| 3 | Git integration & branching — dev workspace ↔ `/fabric`, branch protection, PR template | 🟡 In progress |
| 4 | Bronze — idempotent incremental ingestion (Divvy + Open-Meteo), watermark, metadata | ⬜ |
| 5 | Silver — `src/` PySpark package as wheel, cleansing, dedup, quarantine, station dim | ⬜ |
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
- [ ] `main` protected by a ruleset: PR required, `ci` check required, no force-push / deletion
- [ ] Branch-workspace flow documented (feature branch → branch workspace → PR → main → Update dev from Git)
