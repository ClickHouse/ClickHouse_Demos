# Complete Setup Guide - Observability Workshop

This guide provides detailed step-by-step instructions for setting up the AI-powered observability analytics workshop.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [AWS EC2 Setup](#aws-ec2-setup)
3. [ClickHouse Cloud Setup](#clickhouse-cloud-setup)
4. [Langfuse Cloud Setup](#langfuse-cloud-setup)
5. [LLM Provider Setup](#llm-provider-setup)
6. [Workshop Deployment](#workshop-deployment)
7. [Verification](#verification)
8. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Accounts

- **AWS Account** (for EC2 instance)
- **ClickHouse Cloud Account** (or access to sql.clickhouse.com)
- **Langfuse Cloud Account** (free tier available)
- **LLM Provider** (OpenAI or Anthropic)

### Required Skills

- Basic Linux command line
- SSH access and key management
- Docker basics (helpful but not required)
- SQL fundamentals

### Time Required

- Setup: 30-45 minutes
- Workshop: 75 minutes
- Teardown: 10 minutes

---

## AWS EC2 Setup

### Step 1: Launch EC2 Instance

1. **Navigate to EC2 Console**
   - Go to: https://console.aws.amazon.com/ec2/
   - Select your preferred region (e.g., us-east-1)

2. **Launch Instance**
   - Click "Launch Instance"
   - **Name**: `observability-workshop`

3. **Instance Configuration**
   - **AMI**: Ubuntu Server 22.04 LTS (64-bit x86)
   - **Instance Type**: `t3.medium` (2 vCPU, 4GB RAM) minimum
     - For larger workshops (>10 participants): `t3.large` or `t3.xlarge`
   - **Key Pair**: Create new or select existing

4. **Storage**
   - **Root Volume**: 20 GB gp3 (minimum)
   - 30 GB recommended for production testing

5. **Network Settings**
   - **VPC**: Default (or custom if required)
   - **Auto-assign Public IP**: Enable
   - **Security Group**: Create new with following rules:

   | Type  | Protocol | Port Range | Source    | Description          |
   |-------|----------|------------|-----------|----------------------|
   | SSH   | TCP      | 22         | My IP     | SSH access           |
   | HTTP  | TCP      | 3000       | 0.0.0.0/0 | LibreChat UI         |
   | Custom| TCP      | 4000       | 0.0.0.0/0 | LiteLLM API (optional)|

   **Security Note**: For production, restrict source to specific IP ranges.

6. **Launch Instance**
   - Review settings
   - Click "Launch Instance"
   - Wait for instance state to become "Running"

### Step 2: Connect to EC2 Instance

```bash
# Get instance public IP from AWS console
export EC2_IP=your-instance-public-ip

# Connect via SSH
ssh -i /path/to/your-key.pem ubuntu@$EC2_IP
```

### Step 3: Update System and Install Docker

```bash
# Update system packages
sudo apt-get update && sudo apt-get upgrade -y

# Install prerequisites
sudo apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    git \
    make

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Add current user to docker group
sudo usermod -aG docker $USER

# Apply group changes (or logout/login)
newgrp docker

# Verify Docker installation
docker --version
docker ps

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify Docker Compose
docker-compose --version
```

---

## ClickHouse Cloud Setup

### Option 1: Using sql.clickhouse.com (Demo Data)

If you have access to `sql.clickhouse.com`, you can use the existing `otel_v2` tables:

```bash
# Connection details (example)
CLICKHOUSE_HOST=sql.clickhouse.com
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=your-username
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=default
```

### Option 2: ClickHouse Cloud (Own Instance)

1. **Create Account**
   - Go to: https://clickhouse.cloud/
   - Sign up for free trial (30-day, $300 credit)

2. **Create Service**
   - Click "New Service"
   - **Name**: `observability-workshop`
   - **Provider**: AWS (match EC2 region for lower latency)
   - **Region**: Same as EC2 instance
   - **Tier**: Development (free tier) or Production
   - Click "Create Service"

3. **Get Connection Details**
   - Navigate to "Connect"
   - Copy the following:
     - Host: `xxx.clickhouse.cloud`
     - Port: `8443`
     - Username: `default`
     - Password: (shown once, save it!)

4. **Create Tables**

   Connect using `clickhouse-client`:

   ```bash
   # Install clickhouse-client locally
   curl https://clickhouse.com/ | sh

   # Connect to your instance
   ./clickhouse client \
     --host=your-instance.clickhouse.cloud \
     --port=8443 \
     --user=default \
     --password=your-password \
     --secure

   # Run schema creation
   # Copy/paste from sql/schema.sql
   ```

5. **Load Sample Data** (Optional)

   If you don't have real OpenTelemetry data, you can generate sample data:

   ```bash
   # This will be added in a future update
   # For now, use the tables on sql.clickhouse.com
   ```

---

## Langfuse Cloud Setup

### Step 1: Create Account

1. **Sign Up**
   - Go to: https://cloud.langfuse.com
   - Click "Sign Up"
   - Use email or GitHub authentication

2. **Create Organization**
   - Organization Name: `observability-workshop`
   - Click "Create"

### Step 2: Create Project

1. **New Project**
   - Click "New Project"
   - Project Name: `observability-ai`
   - Click "Create"

### Step 3: Get API Keys

1. **Navigate to Settings**
   - Click on project name
   - Go to "Settings" → "API Keys"

2. **Create API Key**
   - Click "Create New Key"
   - Name: `workshop-key`
   - Copy both keys:
     - **Public Key**: `pk-lf-...`
     - **Secret Key**: `sk-lf-...`

   **IMPORTANT**: Save these keys immediately! Secret key is only shown once.

### Step 4: Configure Tracing

Langfuse will automatically receive traces from LiteLLM once configured. No additional setup needed.

---

## LLM Provider Setup

Choose **ONE** of the following providers:

### Option 1: OpenAI

1. **Create Account**
   - Go to: https://platform.openai.com/signup
   - Complete registration

2. **Add Credit**
   - Go to "Billing" → "Add Payment Method"
   - Add at least $10 for workshop
   - Set usage limits if desired

3. **Create API Key**
   - Go to: https://platform.openai.com/api-keys
   - Click "Create New Secret Key"
   - Name: `observability-workshop`
   - Copy key: `sk-...`

4. **Recommended Models**
   - `gpt-4-turbo-preview` (best quality)
   - `gpt-4o` (fast and cost-effective)
   - `gpt-3.5-turbo` (budget option)

### Option 2: Anthropic Claude

1. **Create Account**
   - Go to: https://console.anthropic.com/
   - Sign up (requires application approval)

2. **Add Credit**
   - Go to "Billing"
   - Add payment method
   - Add at least $10 for workshop

3. **Create API Key**
   - Go to: https://console.anthropic.com/account/keys
   - Click "Create Key"
   - Name: `observability-workshop`
   - Copy key: `sk-ant-...`

4. **Recommended Models**
   - `claude-3-5-sonnet-20241022` (best quality)
   - `claude-3-opus-20240229` (advanced reasoning)
   - `claude-3-haiku-20240307` (fast and cheap)

### Option 3: Google Gemini

1. **Create Account**
   - Go to: https://makersuite.google.com/
   - Sign in with Google account

2. **Get API Key**
   - Go to: https://makersuite.google.com/app/apikey
   - Click "Create API Key"
   - Select or create a Google Cloud project
   - Copy key: `AIzaSy...`

3. **Enable Billing** (optional for higher quotas)
   - Go to Google Cloud Console
   - Enable billing for higher rate limits

4. **Recommended Models**
   - `gemini-1.5-pro` (best quality, 2M context)
   - `gemini-1.5-flash` (fast and cost-effective)
   - `gemini-pro` (standard model)

---

## Workshop Deployment

### Step 1: Clone Repository

```bash
# On your EC2 instance
cd ~
git clone https://github.com/YOUR_USERNAME/clickhouse_ai_observability.git
cd clickhouse_ai_observability/workshop_files
```

### Step 2: Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit with your values
nano .env
```

Fill in all required values:

```bash
# ClickHouse
CLICKHOUSE_HOST=your-instance.clickhouse.cloud
CLICKHOUSE_PORT=8443
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_DATABASE=default

# Langfuse
LANGFUSE_PUBLIC_KEY=pk-lf-xxx
LANGFUSE_SECRET_KEY=sk-lf-xxx
LANGFUSE_HOST=https://cloud.langfuse.com

# LLM Provider (choose one)
OPENAI_API_KEY=sk-xxx
# OR
ANTHROPIC_API_KEY=sk-ant-xxx

# LibreChat
LIBRECHAT_PORT=3000
JWT_SECRET=$(openssl rand -base64 32)
JWT_REFRESH_SECRET=$(openssl rand -base64 32)
MEILI_MASTER_KEY=masterkey123

# LiteLLM
LITELLM_MASTER_KEY=$(openssl rand -base64 32)
DATABASE_URL=postgresql://litellm:litellm123@postgres:5432/litellm
```

Save and exit (`Ctrl+X`, `Y`, `Enter`)

### Step 3: Validate Configuration

```bash
# Test that all required variables are set
make test-env
```

### Step 4: Start Services

```bash
# Pull Docker images and start all services
make start

# This will:
# 1. Pull latest images (2-3 minutes)
# 2. Start containers
# 3. Wait for services to initialize (30 seconds)
# 4. Show status
```

Expected output:
```
✓ Workshop is ready!

Access LibreChat at: http://your-ec2-ip:3000
Access Langfuse at: https://cloud.langfuse.com
```

### Step 5: Verify Services

```bash
# Check all containers are running
make status

# Should show:
# - librechat (running)
# - litellm (running)
# - mongodb (running)
# - postgres (running)
# - meilisearch (running)
```

---

## Verification

### Test 1: ClickHouse Connection

```bash
make test-clickhouse
```

Expected output:
```
✓ ClickHouse connection successful
Version: 24.x.x.x
```

### Test 2: Langfuse Connection

```bash
make test-langfuse
```

Expected output:
```
✓ Langfuse connection successful
```

### Test 3: LiteLLM Health

```bash
make test-llm
```

Expected output:
```
✓ LiteLLM is healthy
```

### Test 4: LibreChat UI

1. **Open Browser**
   - Navigate to: `http://your-ec2-ip:3000`
   - Should see LibreChat login page

2. **Create Account**
   - Click "Sign Up"
   - Email: `workshop@example.com`
   - Password: `Workshop123!`
   - Click "Submit"

3. **Start Conversation**
   - Select model: "Claude 3.5 Sonnet"
   - Type: "What services are currently running?"
   - Should receive SQL query and results

4. **Check Langfuse**
   - Go to: https://cloud.langfuse.com
   - Navigate to your project
   - Click "Traces"
   - Should see trace for your query

### Test 5: End-to-End Query

Run a complete test query:

```
User: "Show me error rates by service in the last hour"

Expected response:
1. SQL query explanation
2. Formatted SQL code block
3. Query results table
4. Insights and recommendations
```

---

## Troubleshooting

### Container Won't Start

```bash
# View logs for specific service
make logs-librechat
make logs-litellm

# Check for port conflicts
sudo netstat -tulpn | grep -E '(3000|4000|27017|5432)'

# Restart services
make restart
```

### ClickHouse Connection Failed

```bash
# Test connection manually
docker exec litellm sh -c 'curl -v "https://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}"'

# Check credentials
cat .env | grep CLICKHOUSE

# Verify security group allows outbound HTTPS (port 8443)
```

### Langfuse Not Receiving Traces

```bash
# Check API keys
cat .env | grep LANGFUSE

# Test Langfuse API
curl -H "Authorization: Bearer $LANGFUSE_PUBLIC_KEY" \
  https://cloud.langfuse.com/api/public/health

# Check LiteLLM logs for errors
docker logs litellm | grep -i langfuse
```

### LLM API Errors

```bash
# Check API key
cat .env | grep API_KEY

# Test OpenAI
curl https://api.openai.com/v1/models \
  -H "Authorization: Bearer $OPENAI_API_KEY"

# Test Anthropic
curl https://api.anthropic.com/v1/messages \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "content-type: application/json" \
  -d '{"model":"claude-3-haiku-20240307","max_tokens":10,"messages":[{"role":"user","content":"Hi"}]}'

# Check rate limits and quotas
```

### LibreChat UI Not Accessible

```bash
# Check container status
docker ps | grep librechat

# Check logs
make logs-librechat

# Verify port is open
curl http://localhost:3000

# Check AWS security group allows inbound on port 3000
```

### Reset Everything

```bash
# Complete reset (preserves .env)
make reset

# Nuclear option (deletes all data)
make clean-volumes
```

---

## Post-Setup Checklist

Before starting the workshop, verify:

- [ ] EC2 instance is running and accessible
- [ ] All Docker containers are healthy (`make status`)
- [ ] ClickHouse connection works (`make test-clickhouse`)
- [ ] Langfuse connection works (`make test-langfuse`)
- [ ] LibreChat UI is accessible
- [ ] Test query completes successfully
- [ ] Trace appears in Langfuse dashboard
- [ ] Sample questions document is ready
- [ ] Workshop slides/materials are prepared

---

## Workshop Day Checklist

1. **30 minutes before**
   - Verify all services are running
   - Test end-to-end query
   - Prepare browser tabs (LibreChat, Langfuse, ClickHouse console)

2. **At start**
   - Share LibreChat URL with participants
   - Demonstrate account creation
   - Run first query together

3. **During workshop**
   - Monitor service health: `make status`
   - Check logs if issues arise
   - Have backup queries ready

4. **After workshop**
   - Export important traces from Langfuse
   - Save chat transcripts if needed
   - Gather feedback

---

## Teardown

### After Workshop

```bash
# Stop services (preserves data)
make stop

# Keep EC2 instance running for post-workshop analysis
```

### Complete Cleanup

```bash
# Stop and remove all containers
make clean

# Remove all data volumes
make clean-volumes

# Terminate EC2 instance (via AWS console)

# Delete ClickHouse Cloud service (if created)

# Archive Langfuse project (optional)
```

### Cost Considerations

Approximate costs for 75-minute workshop:

- **EC2 (t3.medium)**: ~$0.04/hour = $0.05
- **ClickHouse Cloud**: Free tier or ~$0.50
- **Langfuse Cloud**: Free tier
- **LLM API**:
  - OpenAI (GPT-4): ~$2-5 per participant
  - Claude: ~$1-3 per participant
- **Data Transfer**: Negligible

**Total**: $5-10 per participant

---

## Additional Resources

- [ClickHouse Documentation](https://clickhouse.com/docs)
- [LibreChat Documentation](https://docs.librechat.ai/)
- [LiteLLM Documentation](https://docs.litellm.ai/)
- [Langfuse Documentation](https://langfuse.com/docs)
- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)

---

## Support

For issues or questions:
- GitHub Issues: https://github.com/YOUR_USERNAME/clickhouse_ai_observability/issues
- ClickHouse Slack: https://clickhouse.com/slack
- Email workshop organizer

---

**You're now ready to run the workshop!** 🚀
