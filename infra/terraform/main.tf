terraform {
  required_providers {
    aiven = {
      source  = "aiven/aiven"
      version = "~> 4.20"
    }
  }
  required_version = ">= 1.6"
}

provider "aiven" {
  api_token = var.aiven_api_token
}
