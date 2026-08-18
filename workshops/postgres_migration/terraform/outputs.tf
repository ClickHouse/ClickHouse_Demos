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
