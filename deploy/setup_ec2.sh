#!/usr/bin/env bash
# ==============================================================================
# AWS EC2 Free Tier (t2.micro / t3.micro) Provisioning & Deployment Script
# Distributed Ride Dispatcher with Apache Kafka, Redis, and PostgreSQL
# ==============================================================================
set -euo pipefail

echo "=========================================="
echo "Starting EC2 Free Tier System Hardening..."
echo "=========================================="

# 1. Configure 2GB Swap Space (Essential for 1GB RAM instances to prevent OOM)
if [ ! -f /swapfile ]; then
    echo "[1/4] Creating 2GB Swap file..."
    sudo fallocate -l 2G /swapfile || sudo dd if=/dev/zero of=/swapfile bs=1M count=2048
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
    echo "Swap space configured successfully."
else
    echo "[1/4] Swap file already exists. Skipping."
fi

# 2. Kernel & Virtual Memory tuning for Kafka
echo "[2/4] Applying kernel memory limits..."
sudo sysctl -w vm.max_map_count=262144
sudo sysctl -w vm.swappiness=20
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf
echo "vm.swappiness=20" | sudo tee -a /etc/sysctl.conf

# 3. Install Docker & Docker Compose (Ubuntu / Debian / Amazon Linux)
echo "[3/4] Checking Docker installation..."
if ! command -v docker &> /dev/null; then
    echo "Installing Docker..."
    if command -v apt-get &> /dev/null; then
        sudo apt-get update
        sudo apt-get install -y ca-certificates curl gnupg
        sudo install -m 0755 -d /etc/apt/keyrings
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
        sudo chmod a+r /etc/apt/keyrings/docker.gpg
        echo \
          "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
          $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
          sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
        sudo apt-get update
        sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    elif command -v dnf &> /dev/null; then
        sudo dnf update -y
        sudo dnf install -y docker
        sudo systemctl start docker
        sudo systemctl enable docker
    fi
    sudo usermod -aG docker "$USER"
    echo "Docker installed successfully."
else
    echo "Docker is already installed."
fi

# 4. Launch project via Docker Compose
echo "[4/4] Launching Distributed Ride Dispatcher containers..."
docker compose pull || true
docker compose up -d --build

echo "=========================================="
echo "Deployment Complete!"
echo "Services status:"
docker compose ps
echo "=========================================="
echo "Endpoints:"
echo " - Rider Service:     http://<EC2-PUBLIC-IP>:8000"
echo " - Driver REST API:   http://<EC2-PUBLIC-IP>:8001"
echo " - Driver gRPC:       <EC2-PUBLIC-IP>:50051"
echo " - Matching Service:  http://<EC2-PUBLIC-IP>:8002"
echo "=========================================="
