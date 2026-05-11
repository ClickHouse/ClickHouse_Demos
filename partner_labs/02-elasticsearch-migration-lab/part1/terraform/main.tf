# Fetch the latest Amazon Linux 2023 AMI
data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

# Security group for the lab instance
# NOTE: All ingress rules below are open to 0.0.0.0/0 for lab convenience.
# In a production environment, restrict each port to the appropriate CIDR.
resource "aws_security_group" "lab" {
  name        = "${var.lab_name}-sg"
  description = "Security group for ${var.lab_name} lab instance"

  # SSH
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # lab environment — restrict in production
  }

  # Kibana
  ingress {
    description = "Kibana"
    from_port   = 5601
    to_port     = 5601
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # lab environment — restrict in production
  }

  # Elasticsearch HTTP
  ingress {
    description = "Elasticsearch"
    from_port   = 9200
    to_port     = 9200
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # lab environment — restrict in production
  }

  # APM Server
  ingress {
    description = "APM Server"
    from_port   = 8200
    to_port     = 8200
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # lab environment — restrict in production
  }

  # OpenTelemetry gRPC
  ingress {
    description = "OpenTelemetry gRPC"
    from_port   = 4317
    to_port     = 4317
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # lab environment — restrict in production
  }

  # OpenTelemetry HTTP
  ingress {
    description = "OpenTelemetry HTTP"
    from_port   = 4318
    to_port     = 4318
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # lab environment — restrict in production
  }

  # Sample application
  ingress {
    description = "Sample App"
    from_port   = 5000
    to_port     = 5000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # lab environment — restrict in production
  }

  # OTel Demo storefront (frontend-proxy / Envoy)
  ingress {
    description = "OTel Demo Storefront"
    from_port   = 8090
    to_port     = 8090
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # lab environment — restrict in production
  }

  # Allow all outbound traffic
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name      = "${var.lab_name}-sg"
    Lab       = var.lab_name
    ManagedBy = "Terraform"
  }
}

# Lab EC2 instance
resource "aws_instance" "lab" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.instance_type
  key_name               = var.ssh_key_name
  vpc_security_group_ids = [aws_security_group.lab.id]

  root_block_device {
    volume_size = 60
    volume_type = "gp3"
  }

  user_data = templatefile("${path.module}/user-data.sh", {
    lab_dir      = "/home/ec2-user/lab"
    lab_repo_url = var.lab_repo_url
  })

  tags = {
    Name      = var.lab_name
    Lab       = var.lab_name
    ManagedBy = "Terraform"
  }
}
