# AWS EC2 Free Tier Deployment Guide ($0 Setup)

This guide walks you through deploying the **Real-Time Ride Matching Engine** (with Apache Kafka, Redis, PostgreSQL, and FastAPI/gRPC microservices) on an **AWS EC2 `t2.micro` or `t3.micro` instance** completely free under the 12-month AWS Free Tier.

---

## 1. Instance Sizing & The 1GB RAM Challenge

An AWS Free Tier instance (`t2.micro` or `t3.micro`) has:
- **vCPU**: 1
- **RAM**: 1 GB
- **Cost**: $0.00 / month (up to 750 hours/month for 12 months)

### The Solution:
Running Apache Kafka, PostgreSQL, Redis, and 3 Python services simultaneously inside 1 GB RAM requires two optimizations:
1. **KRaft Mode & Memory Capped Kafka**: Apache Kafka runs without ZooKeeper, and its JVM heap is capped to `-Xms128m -Xmx256m` in `docker-compose.yml`.
2. **2 GB Linux Swap File**: Virtual RAM on the EBS hard disk absorbs sudden memory spikes during builds or traffic bursts, preventing Linux from killing processes (OOM).

---

## 2. Launching the EC2 Instance (Step-by-Step)

1. **Log in to AWS Console** and navigate to **EC2 Dashboard**.
2. Click **Launch Instance**:
   - **Name**: `ride-matching-engine`
   - **OS Image**: **Ubuntu 24.04 LTS** (Free tier eligible) or **Amazon Linux 2023**.
   - **Instance Type**: `t2.micro` (or `t3.micro` in regions where available).
   - **Key Pair**: Select or create an `.pem` key pair (e.g. `ride-key.pem`).
3. **Configure Network / Security Group**:
   Create a Security Group with the following inbound rules:
   | Protocol | Port | Source | Description |
   | :--- | :--- | :--- | :--- |
   | **SSH** | `22` | My IP (or `0.0.0.0/0`) | Remote terminal access |
   | **Custom TCP** | `8000` | `0.0.0.0/0` | Rider Service REST API |
   | **Custom TCP** | `8001` | `0.0.0.0/0` | Driver Service REST API |
   | **Custom TCP** | `50051` | `0.0.0.0/0` | Driver Telemetry gRPC Streaming |
   | **Custom TCP** | `8002` | `0.0.0.0/0` | Matching Service API |

   *(Note: Kafka port `9092`, Postgres port `5432`, and Redis port `6379` remain internal to the Docker network and are NOT exposed to the public internet for security).*

4. Click **Launch Instance**.

---

## 3. One-Click Provisioning & Deployment

1. **SSH into your EC2 instance**:
   ```bash
   ssh -i /path/to/ride-key.pem ubuntu@<EC2-PUBLIC-IP>
   ```

2. **Clone your repository**:
   ```bash
   git clone https://github.com/<YOUR-USERNAME>/Distributed-Ride-Dispatcher.git
   cd Distributed-Ride-Dispatcher
   ```

3. **Run the Automated Setup Script**:
   ```bash
   chmod +x deploy/setup_ec2.sh
   ./deploy/setup_ec2.sh
   ```

   This script automatically:
   - Configures the 2 GB Swap file.
   - Installs Docker and Docker Compose.
   - Starts all 6 containers (`kafka`, `postgres`, `redis`, `driver-service`, `matching-service`, `rider-service`).

---

## 4. Verifying Your Live Deployment

### A. Check Container Health
```bash
docker compose ps
```
All containers should report `Up` or `healthy`.

### B. Test the Rider API (from your local terminal or Postman)
```bash
curl http://<EC2-PUBLIC-IP>:8000/health
# Response: {"status":"ok","service":"rider-service","kafka_connected":true}
```

### C. Create a Ride Request
```bash
curl -X POST http://<EC2-PUBLIC-IP>:8000/rides/request \
  -H "Content-Type: application/json" \
  -d '{"rider_id":"rider_alice","pickup_lat":37.7749,"pickup_lon":-122.4194}'
```

### D. Run the Driver Fleet Simulator from Your Laptop Against AWS
You can run the simulated fleet of drivers on your laptop while streaming telemetry to your live EC2 instance over gRPC:
```bash
python3 simulator/simulate_drivers.py --drivers 100 --duration 30 --target <EC2-PUBLIC-IP>:50051
```
