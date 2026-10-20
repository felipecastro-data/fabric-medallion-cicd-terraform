# Project plan & progress

**Progress: Step 1 of 8 — 5%**

| # | Step | Status |
|---|------|--------|
| 1 | Foundations — repo, local tools, Entra app + GitHub OIDC, Fabric tenant settings, HCP Terraform | 🟡 In progress |
| 2 | Terraform — dev/test/prod workspaces, lakehouses, environment, roles, deployment pipeline, TF CI | ⬜ |
| 3 | Git integration & branching — dev workspace ↔ `/fabric`, branch protection, PR template | ⬜ |
| 4 | Bronze — idempotent incremental ingestion (Divvy + Open-Meteo), watermark, metadata | ⬜ |
| 5 | Silver — `src/` PySpark package as wheel, cleansing, dedup, quarantine, station dim | ⬜ |
| 6 | Gold & data quality — star schema, aggregates, `dq_results`, pytest + chispa | ⬜ |
| 7 | Orchestration & CD — Data Pipeline, GitHub Actions CI/CD, Deployment Pipeline, approvals, smoke tests | ⬜ |
| 8 | Observability & showcase — run log, DQ trends, failure alerts, README screenshots, LinkedIn post | ⬜ |

## Step 1 checklist

- [x] Public GitHub repo created
- [x] Local tools: Homebrew, Terraform, Azure CLI, gh, uv, Java 17
- [x] Repo skeleton committed
- [ ] Entra app registration + GitHub OIDC federated credentials
- [ ] Fabric tenant settings for service principals
- [ ] HCP Terraform organization + workspaces
