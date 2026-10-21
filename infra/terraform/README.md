# Platform infrastructure (Terraform)

Provisions the Microsoft Fabric platform for every environment with the
[`microsoft/fabric`](https://registry.terraform.io/providers/microsoft/fabric/latest) provider.

| Resource | Per environment (dev / test / prod) | Shared |
|----------|-------------------------------------|--------|
| Workspace on the Fabric capacity | `divvy-medallion-<env>` | |
| Lakehouses (schemas enabled, `prevent_destroy`) | `lh_bronze`, `lh_silver`, `lh_gold` | |
| Spark workspace settings | Runtime 1.3, auto-logging, sized starter pool, session timeout | |
| Role assignments | Human admin + CI service principal | |
| Deployment pipeline | | Development → Test → Production |

**Ownership split:** Terraform owns the *platform* (workspaces, storage, compute settings, access, promotion path).
Git integration and the deployment pipeline own the *code* (notebooks, data pipelines, environments).
Lakehouses are created before the workspaces are assigned to pipeline stages, so the pipeline pairs them by name.

## How it runs

- **State:** HCP Terraform workspace `fabric-medallion-platform` (execution mode *Local*: state only).
- **Auth:** GitHub OIDC → Entra service principal `sp-fabric-medallion-cicd`; no client secrets anywhere.
- **Pull request:** `fmt` · `validate` · `tflint` · `plan` posted as a PR comment.
- **Merge to `main`:** `apply` after manual approval on the `platform` GitHub environment.

## Run locally (plan only)

```bash
az login --allow-no-subscriptions --tenant <tenant>
terraform login                      # HCP Terraform
cat > local.auto.tfvars <<'VARS'      # gitignored
capacity_id          = "<fabric-capacity-id>"
admin_user_object_id = "<entra-user-object-id>"
VARS
terraform init && terraform plan
```
