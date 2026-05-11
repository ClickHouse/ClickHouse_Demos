#!/bin/bash
set -euo pipefail

# Install Docker
dnf update -y
dnf install -y docker git
systemctl start docker
systemctl enable docker
usermod -aG docker ec2-user

# Install Docker Compose v2
mkdir -p /usr/local/lib/docker/cli-plugins
curl -SL "https://github.com/docker/compose/releases/download/v2.24.5/docker-compose-linux-x86_64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
ln -s /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/bin/docker-compose

# Increase vm.max_map_count for Elasticsearch
echo "vm.max_map_count=262144" >> /etc/sysctl.conf
sysctl -w vm.max_map_count=262144

# Clone the lab repo
LAB_REPO="${lab_repo_url}"
LAB_DIR="/home/ec2-user/lab"
git clone "$LAB_REPO" "$LAB_DIR" || {
  echo "Could not clone repo, creating directory structure..."
  mkdir -p "$LAB_DIR"
}
chown -R ec2-user:ec2-user "$LAB_DIR"

# Start the source stack
cd "$LAB_DIR/part1/docker"
sudo -u ec2-user docker compose -f docker-compose.source.yml up -d

echo "Lab setup complete. Services starting..."
