output "workspace_id" {
  description = "Workspace ID."
  value       = fabric_workspace.this.id
}

output "lakehouse_ids" {
  description = "Lakehouse IDs keyed by name."
  value       = { for name, lh in fabric_lakehouse.this : name => lh.id }
}
