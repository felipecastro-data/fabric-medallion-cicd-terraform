# Working on this repository

Microsoft Fabric medallion lakehouse (Divvy trips + Open-Meteo weather) with Terraform, GitHub Actions and Fabric
deployment pipelines. Progress and step checklists: [docs/PLAN.md](docs/PLAN.md). Branching model:
[docs/development-workflow.md](docs/development-workflow.md).

## Start of every session — verify, don't ask

Credentials persist on this Mac; sessions reuse them. Before asking for any access, tokens or accounts, check:

```bash
eval "$(/opt/homebrew/bin/brew shellenv zsh)"
az account show --query user.name -o tsv          # new_user_fabric@felipe9393icloud384.onmicrosoft.com
gh auth status                                     # felipecastro-data
ls ~/.terraform.d/credentials.tfrc.json            # HCP Terraform (org felipecastro-data)
az rest --method get --resource https://api.fabric.microsoft.com \
  --url https://api.fabric.microsoft.com/v1/workspaces --query "value[].[displayName,id]" -o tsv
```

If a workspace is missing, say so precisely and ask the user to confirm the signed-in account (top right in Fabric) and
whether it was created — never ask for new credentials. Local Spark tests need `JAVA_HOME=/opt/homebrew/opt/openjdk@17`.

## Rules

- `main` is protected (ruleset `protect-main`): PR, green `ci`, all conversations resolved, squash merge. Work on a
  feature branch; Fabric work happens in a branch workspace (`divvy-medallion-feat-*`).
- Validate every review finding; fix real ones with tests, rebut outdated ones with docs. Ask the user before posting
  PR replies from their account.
- Changing `src/` → bump the version in `pyproject.toml` and run `python tools/sync_environment_wheel.py`
  (CI checks the wheel inside `fabric/env_divvy.Environment` byte-for-byte).
- Breaking schema change in a Silver/Gold table → run the notebook once with `rebuild=True` (never silent type changes).
- Notebook parameter defaults must stay production values (`periods="last:12"`, `force=False`); a test enforces it.

## Fabric quirks (learned the hard way)

- Git integration and Branch out only **stage** Environments: publish afterwards
  (`python tools/publish_environment_library.py --workspace-id <ws>`).
- `.platform` files have **no trailing newline**; notebook markdown cells must not contain lone `#` lines.
- Rehearse CD steps with `tools/fabric_ops.py` (update from Git, run notebook jobs) and verify data with
  `tools/read_onelake.py` (delta-rs over OneLake) instead of asking for screenshots.
