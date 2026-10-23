data "fabric_capacity" "this" {
  id = var.capacity_id

  lifecycle {
    postcondition {
      condition     = self.state == "Active"
      error_message = "The Fabric capacity is not Active (paused or expired trial)."
    }
  }
}

module "workspace" {
  source   = "./modules/fabric_workspace_stack"
  for_each = var.environments

  display_name            = "${var.project_name}-${each.key}"
  description             = "Divvy medallion lakehouse — ${each.value.stage_display_name} environment (managed by Terraform)"
  capacity_id             = data.fabric_capacity.this.id
  admin_user_object_id    = var.admin_user_object_id
  lakehouses              = var.lakehouses
  starter_pool_max_nodes  = each.value.starter_pool_max_nodes
  session_timeout_minutes = each.value.session_timeout_minutes
}

locals {
  ordered_envs = [for env in sort([for k, v in var.environments : format("%02d-%s", v.stage_order, k)]) : split("-", env)[1]]
}

# Lakehouses must exist in every workspace BEFORE the workspaces are assigned to stages,
# so the deployment pipeline pairs them by name instead of creating duplicates.
resource "fabric_deployment_pipeline" "this" {
  display_name = "${var.project_name}-pipeline"
  description  = "Promotes Fabric items Development -> Test -> Production (managed by Terraform)"

  stages = [
    for env in local.ordered_envs : {
      display_name = var.environments[env].stage_display_name
      description  = "${var.environments[env].stage_display_name} stage"
      is_public    = false
      workspace_id = module.workspace[env].workspace_id
    }
  ]

  depends_on = [module.workspace]
}

resource "fabric_deployment_pipeline_role_assignment" "admin_user" {
  deployment_pipeline_id = fabric_deployment_pipeline.this.id
  role                   = "Admin"

  principal = {
    id   = var.admin_user_object_id
    type = "User"
  }
}

# Weekly prod run of the orchestration pipeline. Divvy publishes each month a few days after it ends; every step is
# incremental, so a weekly run picks up a new month within days and is a cheap no-op otherwise.
data "fabric_data_pipeline" "prod_medallion" {
  count = var.prod_schedule_enabled ? 1 : 0

  workspace_id = module.workspace["prod"].workspace_id
  display_name = "pl_medallion"
}

resource "fabric_item_job_scheduler" "prod_medallion_weekly" {
  count = var.prod_schedule_enabled ? 1 : 0

  workspace_id = module.workspace["prod"].workspace_id
  item_id      = data.fabric_data_pipeline.prod_medallion[0].id
  job_type     = "Execute" # the provider (v1.14) accepts only Execute for DataPipeline schedules
  enabled      = true

  configuration = {
    type               = "Weekly"
    weekdays           = ["Monday"]
    times              = ["06:00"]
    local_time_zone_id = "Central Standard Time" # Chicago, where the data comes from
    start_date_time    = "2026-10-26T00:00:00Z"
    end_date_time      = "2027-12-31T00:00:00Z"
  }
}
