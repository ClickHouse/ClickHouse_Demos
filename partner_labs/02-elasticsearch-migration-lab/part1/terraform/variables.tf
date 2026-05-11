variable "aws_region" {
  description = "AWS region to deploy to"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 instance type (t3.2xlarge recommended: 8 vCPU / 32 GB — required for dual workload)"
  type        = string
  default     = "t3.2xlarge"
}

variable "ssh_key_name" {
  description = "Name of the EC2 key pair for SSH access"
  type        = string
}

variable "allowed_ssh_cidr" {
  description = "CIDR block allowed to SSH (your IP + /32)"
  type        = string
  default     = "0.0.0.0/0"
}

variable "lab_name" {
  description = "Name tag for all resources"
  type        = string
  default     = "clickhouse-es-migration-lab"
}

variable "lab_repo_url" {
  description = "Git repository URL containing the lab files (used by EC2 user-data to clone the repo)"
  type        = string
  default     = "https://github.com/ClickHouse/clickhouse-partner-labs.git"
}
