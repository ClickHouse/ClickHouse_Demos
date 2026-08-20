variable "region" {
  description = "AWS region. Must be a region where ClickHouse Managed Postgres is available -- see module 00."
  type        = string
  default     = "us-east-2"
}

variable "name_prefix" {
  description = "Prefix for every created resource, so one AWS account can host several participants."
  type        = string
  default     = "pgmig"
}

variable "instance_class" {
  description = "RDS instance class. Sized so the fact table exceeds RAM, which is what makes the dashboard queries disk-bound."
  type        = string
  default     = "db.m6g.large"
}

variable "allocated_storage_gb" {
  description = "gp3 storage. Left at default IOPS on purpose: over-provisioning IOPS would make the baseline unrepresentative."
  type        = number
  default     = 200
}

# ClickPipes reads FROM this instance, so it dials IN from its own documented static egress
# addresses -- not from the Managed Postgres instance, which is a common wrong guess because the
# hand-rolled logical-replication version of this leg did work that way. Set this to the
# ClickPipes addresses for your region plus your own /32. Participant-supplied and deliberately
# explicit; there is no default that would be both correct and safe.
variable "subscriber_cidrs" {
  description = "CIDRs allowed to reach Postgres on 5432. Set this to the ClickPipes static egress addresses for your region, plus your own /32."
  type        = list(string)
}

variable "db_name" {
  description = "Database name."
  type        = string
  default     = "shop"
}

variable "admin_username" {
  description = "Master username."
  type        = string
  default     = "shopadmin"
}
