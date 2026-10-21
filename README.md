# Fabric Medallion Lakehouse — Terraform · CI/CD · Data Quality

> 🚧 **Work in progress** — see [docs/PLAN.md](docs/PLAN.md) for live progress.

Production-grade data engineering project on **Microsoft Fabric**: a bronze / silver / gold lakehouse built on
real Chicago **Divvy** bike-share trips enriched with **Open-Meteo** weather, provisioned with **Terraform**,
promoted Dev → Test → Prod through **GitHub Actions** and **Fabric Deployment Pipelines**, and guarded by
**PySpark unit tests** and **data-quality checks**.

## Architecture

```mermaid
flowchart LR
    subgraph Sources
        S1[Divvy trip data<br/>monthly files]
        S2[Open-Meteo<br/>weather API]
    end
    subgraph Fabric["Microsoft Fabric workspace (dev / test / prod)"]
        B[(lh_bronze<br/>raw Delta)]
        SV[(lh_silver<br/>clean · deduped · quarantine)]
        G[(lh_gold<br/>star schema · KPIs · dq_results)]
        P{{Data Pipeline<br/>orchestration}}
    end
    S1 --> B
    S2 --> B
    B --> SV --> G
    P -.runs.-> B & SV & G
```

```mermaid
flowchart LR
    Dev[Feature branch] -->|PR: lint · pytest · terraform plan| Main[main]
    Main -->|sync| WSD[Dev workspace]
    WSD -->|Deployment Pipeline| WST[Test workspace]
    WST -->|manual approval| WSP[Prod workspace]
```

## Tech stack

| Area | Tools |
|------|-------|
| Platform | Microsoft Fabric (Lakehouse, Notebooks, Data Pipelines, Environments) |
| Processing | PySpark 3.5 / Delta Lake |
| Infrastructure as Code | Terraform (`microsoft/fabric` provider), HCP Terraform remote state |
| CI/CD | GitHub Actions (OIDC, no secrets), Fabric Git integration, Fabric Deployment Pipelines |
| Testing | pytest, chispa, rules-based data-quality framework |

## Screenshots

### Infrastructure as Code

| Terraform plan posted on every pull request | Apply gated by a manual approval |
|---|---|
| ![Terraform plan PR comment](docs/screenshots/01-terraform-pr-plan.webp) | ![Approval-gated terraform apply](docs/screenshots/02-terraform-apply-approval.webp) |

| Workspaces and lakehouses created by the CI service principal | Deployment pipeline with lakehouses paired across stages |
|---|---|
| ![Fabric workspace with medallion lakehouses](docs/screenshots/03-workspace-lakehouses.webp) | ![Fabric deployment pipeline Development → Test → Production](docs/screenshots/04-deployment-pipeline.webp) |

## Repository layout

```
.github/workflows/   ci (lint · tests · Fabric item validation), terraform (plan/apply), CD (Dev → Test → Prod)
infra/terraform/     Workspaces, lakehouses, environments, role assignments, deployment pipeline
fabric/              Fabric items synced through Git integration
src/divvy_medallion/ PySpark transformations and data-quality rules (built as a wheel)
tests/               Unit tests and data-quality tests
tools/               Repo tooling (Fabric item validator used in CI)
docs/                Plan, architecture and screenshots
```

## License

[MIT](LICENSE)
