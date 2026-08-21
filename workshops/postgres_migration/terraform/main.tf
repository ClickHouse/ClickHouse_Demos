data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "random_password" "admin" {
  length  = 24
  special = false
}

resource "aws_db_parameter_group" "shop" {
  name_prefix = "${var.name_prefix}-pg17-"
  family      = "postgres17"
  description = "Logical replication enabled so this instance can be a publication source."

  # Static parameter: RDS requires an instance reboot before it takes effect. Module 01
  # verifies with `SHOW wal_level` returning `logical`.
  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }

  # One slot and one sender would do for this workshop, but a participant who retries the
  # subscription without dropping the old slot needs headroom, and an exhausted slot limit
  # fails with a message that reads nothing like its cause.
  parameter {
    name         = "max_replication_slots"
    value        = "10"
    apply_method = "pending-reboot"
  }

  parameter {
    name         = "max_wal_senders"
    value        = "10"
    apply_method = "pending-reboot"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "shop" {
  name_prefix = "${var.name_prefix}-rds-"
  description = "Postgres access for the workshop participant and the Managed Postgres subscriber."
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Postgres from the participant and the subscriber"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = var.subscriber_cidrs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_db_subnet_group" "shop" {
  name_prefix = "${var.name_prefix}-"
  subnet_ids  = data.aws_subnets.default.ids
}

resource "aws_db_instance" "shop" {
  identifier_prefix = "${var.name_prefix}-"
  engine            = "postgres"
  engine_version    = "17"
  instance_class    = var.instance_class

  allocated_storage = var.allocated_storage_gb
  storage_type      = "gp3"

  db_name  = var.db_name
  username = var.admin_username
  password = random_password.admin.result

  parameter_group_name   = aws_db_parameter_group.shop.name
  db_subnet_group_name   = aws_db_subnet_group.shop.name
  vpc_security_group_ids = [aws_security_group.shop.id]

  # The subscription is created on the TARGET, so Managed Postgres must be able to reach
  # this instance. Narrowed by the security group above, not by privacy.
  publicly_accessible = true

  # NOT zero. RDS requires automated backups to be enabled for logical replication, because
  # that is what gives it long-term WAL retention -- with retention at 0 the publication is
  # created successfully and the subscription then never receives anything, which is a much
  # worse failure than a refusal. One day is the cheapest value that satisfies it.
  backup_retention_period      = 1
  skip_final_snapshot          = true
  apply_immediately            = true
  performance_insights_enabled = true
}
