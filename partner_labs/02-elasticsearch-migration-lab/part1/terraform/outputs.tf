output "instance_public_ip" {
  description = "Public IP of the lab EC2 instance"
  value       = aws_instance.lab.public_ip
}

output "instance_public_dns" {
  description = "Public DNS of the lab EC2 instance"
  value       = aws_instance.lab.public_dns
}

output "ssh_command" {
  description = "SSH command to connect to the instance"
  value       = "ssh -i ~/.ssh/${var.ssh_key_name}.pem ec2-user@${aws_instance.lab.public_ip}"
}

output "kibana_url" {
  description = "Kibana URL"
  value       = "http://${aws_instance.lab.public_ip}:5601"
}

output "elasticsearch_url" {
  description = "Elasticsearch URL"
  value       = "http://${aws_instance.lab.public_ip}:9200"
}
