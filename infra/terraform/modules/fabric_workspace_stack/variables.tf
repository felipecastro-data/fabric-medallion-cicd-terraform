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
  description = "Maximum node count for the starter Spark pool."
  type        = number
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
