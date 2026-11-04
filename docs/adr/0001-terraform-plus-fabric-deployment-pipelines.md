# ADR 0001 — Terraform for the platform, Fabric deployment pipelines for promotion

- **Status:** Accepted
- **Date:** 2026-10-20

## Context

Three environments (dev, test, prod) must stay identical in shape: the same workspaces, lakehouses, Spark settings
and roles, plus the same notebooks, pipeline and Spark environment. Two kinds of things move through them:

- **Platform**: workspaces, capacity assignment, lakehouses, Spark pools, role assignments, schedules. These change
  rarely, and a mistake can destroy data.
- **Items**: notebooks, `pl_medallion`, `env_divvy`. These change on every feature and must be promoted only after
  they have run successfully in the stage before.

There is no Azure subscription (trial capacity, HCP Terraform for state). Every automated call must go through GitHub
OIDC, with no stored secrets.

## Options

| | Option | For | Against |
|---|---|---|---|
| A | **Terraform for everything** (platform + `fabric_notebook` / `fabric_data_pipeline` items) | One tool, one plan | Item definitions are duplicated between Terraform and Git. A notebook edited in Fabric drifts from state. Every notebook change becomes an infra apply. |
| B | **Terraform + `fabric-cicd`** (Python library deploying item definitions from the repo to each workspace) | Deploys exactly the commit; parameter files per environment; no Fabric-side state | Workspace and lakehouse IDs must be rewritten through `parameter.yml`. Another tool to pin and upgrade. No built-in pairing or diff UI for reviewers in Fabric. |
| C | **Git integration per workspace** (dev → `main`, test → `release`, prod → `prod` branch) | Promotion is a merge | Long-lived branches drift. Lakehouse bindings differ per branch. An approval is only a PR approval. A bad merge to `prod` deploys immediately. |
| D | **Terraform for the platform, Git integration on dev only, Fabric deployment pipeline dev → test → prod, orchestrated by GitHub Actions** | Each tool does what it is good at. The pipeline pairs lakehouses by name, so notebooks have no environment-specific IDs. The GitHub `prod` environment adds a human approval. The Fabric UI shows the diff between stages. | Fabric-specific. The pipeline promotes what is *in* test, not a commit. Environments are only *staged* by Git and deployments, and need an explicit publish. |

## Decision

**Option D.** Terraform (`microsoft/fabric`) owns workspaces, lakehouses (`prevent_destroy`), Spark pool sizing, role
assignments, the deployment pipeline and the prod schedule. The `deploy` workflow syncs dev from Git (pinned to
`GITHUB_SHA`), then promotes with the deployment pipeline. After each step it publishes `env_divvy` and runs
`pl_medallion`, so a stage is promoted only after the previous stage ran green. Prod waits for a required reviewer.

## Consequences

- Lakehouses must exist in every workspace **before** the workspaces are assigned to stages, or the pipeline creates
  duplicates instead of pairing them (`depends_on` in `infra/terraform/main.tf`).
- "What is in test" equals "the commit just deployed" only because deploys are serialized (`concurrency: deploy`) and
  dev refuses any commit other than the branch head.
- The default-environment binding is owned by the workflow, not Terraform (`ignore_changes = [environment]`), because
  `env_divvy` exists only after the first deployment.
- **Revisit when:** more than a handful of item types need per-environment parameters, or the team wants to deploy
  a tagged commit straight to prod. `fabric-cicd` (B) then becomes the better fit, and the platform half stays as is.
