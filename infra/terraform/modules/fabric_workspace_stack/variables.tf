variable "display_name" {
  description = "Workspace display name."
  type        = string
}

variable "description" {
  description = "Workspace description."
  type        = string
}

variable "capacity_id" {
  description = "Fabric capacity ID the workspace is assigned to."
  type        = string
}

variable "admin_user_object_id" {
  description = "Entra object ID of the user granted the Admin role."
  type        = string
}

variable "lakehouses" {
  description = "Map of lakehouse name => description."
  type        = map(string)
}

variable "starter_pool_max_nodes" {
  description = "Maximum node count for the starter Spark pool (driver + executors, so at least 2)."
  type        = number

  validation {
    condition     = var.starter_pool_max_nodes >= 2
    error_message = "A starter pool needs at least 2 nodes: one for the driver and one per executor."
  }
}

variable "session_timeout_minutes" {
  description = "Idle Spark session timeout in minutes."
  type        = number
}

variable "runtime_version" {
  description = "Fabric Spark runtime version (1.3 = Spark 3.5 / Python 3.11)."
  type        = string
  default     = "1.3"
}
