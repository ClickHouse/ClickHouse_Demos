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

# ClickHouse Managed Postgres creates the SUBSCRIPTION, so it dials OUT to this instance.
# Narrow this to ClickHouse Cloud's documented egress addresses once confirmed (open item 1
# in the design doc). Until then it is participant-supplied and deliberately explicit.
variable "subscriber_cidrs" {
  description = "CIDRs allowed to reach Postgres on 5432. Set this to the ClickHouse Cloud egress range for your region, plus your own /32."
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
