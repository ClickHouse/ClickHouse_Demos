output "rds_endpoint" {
  description = "host:port for psql and for the subscription's CONNECTION string."
  value       = "${aws_db_instance.shop.address}:${aws_db_instance.shop.port}"
}

output "rds_database" {
  value = aws_db_instance.shop.db_name
}

output "rds_admin_user" {
  value = aws_db_instance.shop.username
}

output "rds_admin_password" {
  value     = random_password.admin.result
  sensitive = true
}

output "parameter_group_name" {
  value = aws_db_parameter_group.shop.name
}

# identifier_prefix generates the name at apply time, so it cannot be written down in advance --
# but it IS in state. Reading it from here is exact and needs no API call, which is why nothing in
# the playbook filters `aws rds describe-db-instances` by endpoint to recover it.
output "rds_instance_id" {
  description = "Generated DB instance identifier, for reboot-db-instance and the CloudWatch dimension."
  value       = aws_db_instance.shop.identifier
}

# The AWS CLI does not inherit the provider's region. Export this as AWS_REGION so every raw `aws`
# call in modules 01, 03 and 06 targets the region this stack was actually applied into.
output "region" {
  description = "Region this stack was applied into. Export as AWS_REGION."
  value       = var.region
}
