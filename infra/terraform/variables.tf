variable "project_name" {
  description = "Short project name, used as the prefix for every Fabric workspace."
  type        = string
  default     = "divvy-medallion"
}

variable "capacity_id" {
  description = "ID of the Fabric capacity that hosts all workspaces."
  type        = string
}

variable "admin_user_object_id" {
  description = "Entra object ID of the human administrator granted Admin on every workspace and the deployment pipeline."
  type        = string
}

variable "lakehouses" {
  description = "Medallion lakehouses created in every environment. Names are identical across environments so notebooks stay environment-agnostic."
  type        = map(string)
  default = {
    lh_bronze = "Bronze layer: raw, append-only Delta tables with ingestion metadata"
    lh_silver = "Silver layer: typed, validated, deduplicated data plus quarantine tables"
    lh_gold   = "Gold layer: star schema, aggregates and data-quality results"
  }
}

variable "environments" {
  description = "Per-environment settings. Order of the deployment pipeline stages follows `stage_order`."
  type = map(object({
    stage_order             = number
    stage_display_name      = string
    starter_pool_max_nodes  = number
    session_timeout_minutes = number
  }))
  default = {
    dev = {
      stage_order             = 0
      stage_display_name      = "Development"
      starter_pool_max_nodes  = 2
      session_timeout_minutes = 20
    }
    test = {
      stage_order             = 1
      stage_display_name      = "Test"
      starter_pool_max_nodes  = 2
      session_timeout_minutes = 20
    }
    prod = {
      stage_order             = 2
      stage_display_name      = "Production"
      starter_pool_max_nodes  = 4
      session_timeout_minutes = 30
    }
  }

  validation {
    condition     = alltrue([for k in keys(var.environments) : contains(["dev", "test", "prod"], k)])
    error_message = "Environment keys must be dev, test or prod."
  }
}
