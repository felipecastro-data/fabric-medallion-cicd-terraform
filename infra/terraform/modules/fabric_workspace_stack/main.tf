resource "fabric_workspace" "this" {
  display_name = var.display_name
  description  = var.description
  capacity_id  = var.capacity_id
}

# The identity running Terraform (the CI service principal) becomes Admin as creator;
# the human administrator is granted Admin explicitly.
resource "fabric_workspace_role_assignment" "admin_user" {
  workspace_id = fabric_workspace.this.id
  role         = "Admin"

  principal = {
    id   = var.admin_user_object_id
    type = "User"
  }
}

resource "fabric_lakehouse" "this" {
  for_each = var.lakehouses

  workspace_id = fabric_workspace.this.id
  display_name = each.key
  description  = each.value

  configuration = {
    enable_schemas = true
  }

  # Lakehouses hold data: never let a plan silently destroy and recreate them.
  lifecycle {
    prevent_destroy = true
  }
}

resource "fabric_spark_workspace_settings" "this" {
  workspace_id = fabric_workspace.this.id

  automatic_log = {
    enabled = true
  }

  environment = {
    runtime_version = var.runtime_version
  }

  job = {
    session_timeout_in_minutes = var.session_timeout_minutes
  }

  pool = {
    starter_pool = {
      max_node_count = var.starter_pool_max_nodes
      max_executors  = var.starter_pool_max_nodes
    }
  }
}
