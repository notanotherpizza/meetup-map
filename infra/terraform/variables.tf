variable "aiven_api_token" {
  description = "Aiven API token — generate at console.aiven.io → User profile → Tokens"
  type        = string
  sensitive   = true
}

variable "aiven_project" {
  description = "Aiven project name"
  type        = string
  default     = "hugh-one"
}

variable "cloud_name" {
  description = "Aiven cloud region (must be Aiven Apps compatible)"
  type        = string
  default     = "aws-eu-west-1"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "192.168.0.0/24"
}
