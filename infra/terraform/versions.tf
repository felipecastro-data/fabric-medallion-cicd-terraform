terraform {
  required_version = ">= 1.11, < 2.0"

  # Remote state in HCP Terraform (execution mode: Local — plans/applies run in GitHub Actions)
  cloud {
    organization = "felipecastro-data"

    workspaces {
      name = "fabric-medallion-platform"
    }
  }

  required_providers {
    fabric = {
      source  = "microsoft/fabric"
      version = "~> 1.14"
    }
  }
}

# Authentication is supplied through environment variables, never in code:
#   CI    -> FABRIC_USE_OIDC=true, FABRIC_CLIENT_ID, FABRIC_TENANT_ID (GitHub OIDC, no secrets)
#   local -> Azure CLI user context (`az login`)
provider "fabric" {}
