output "workspace_ids" {
  description = "Workspace ID per environment."
  value       = { for env, ws in module.workspace : env => ws.workspace_id }
}

output "lakehouse_ids" {
  description = "Lakehouse IDs per environment."
  value       = { for env, ws in module.workspace : env => ws.lakehouse_ids }
}

output "deployment_pipeline_id" {
  description = "Deployment pipeline ID (used by the CD workflow)."
  value       = fabric_deployment_pipeline.this.id
}

output "deployment_pipeline_stage_ids" {
  description = "Deployment pipeline stage IDs keyed by stage display name."
  value       = { for s in fabric_deployment_pipeline.this.stages : s.display_name => s.id }
}
