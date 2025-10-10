# Databricks on Azure - Terraform Infrastructure

Complete Terraform infrastructure for deploying Databricks workspaces on Azure with medallion architecture (landing, bronze, silver, gold).

---

## 📋 Quick Start

```bash
# 1. Initialize Terraform
terraform init

# 2. Plan deployment (review changes)
terraform plan -var-file=dev.tfvars

# 3. Deploy infrastructure
terraform apply -var-file=dev.tfvars -auto-approve

# 4. Destroy everything (when done)
terraform destroy -var-file=dev.tfvars -auto-approve
```

---

## 🏗️ Two-Stage Deployment Architecture

**★ Insight ─────────────────────────────────────**
The deployment uses a **two-stage architecture** to handle Azure and Databricks resource dependencies:

### Stage 1: Azure Infrastructure
Creates the foundation resources that Databricks needs to exist:
- Resource Group
- Virtual Network with delegated subnets
- Storage Accounts (ADLS Gen2 + ML)
- Networking (NSG, subnet associations)
- Monitoring (Log Analytics, Application Insights)
- Security (Key Vault)
- **Databricks Workspace** (takes 3-5 minutes to provision)

### Stage 2: Databricks-Native Resources
After the workspace exists, deploys Databricks-specific resources that require workspace authentication:
- Unity Catalog (catalogs, schemas, external locations)
- Compute (clusters, SQL warehouses)
- User Groups and RBAC permissions
- Service Principals
- Secret Scopes
- Cluster Policies

**Why Two Stages?**
- Databricks provider needs `workspace_url` which doesn't exist until Stage 1 completes
- Authentication to workspace required for Stage 2 resources
- Prevents chicken-and-egg dependency issues
─────────────────────────────────────────────────

---

## 📁 Project Structure

```
terraform/
├── README.md                    # This file
├── providers.tf                 # Provider configurations (Azure, Databricks, Random)
├── locals.tf                    # Environment config and feature flags
├── variables.tf                 # Input variables with validation
├── outputs.tf                   # Outputs (workspace URL, storage, catalog names)
├── infrastructure.tf            # Stage 1: Azure networking, storage, workspace
├── databricks.tf                # Stage 2: Clusters, SQL warehouses, user groups
├── security.tf                  # Stage 2: Service principals, secret scopes
├── unity_catalog.tf             # Stage 2: Unity Catalog resources (if enabled)
├── dev.tfvars                   # Development environment overrides
├── prod.tfvars                  # Production environment configuration
├── credentials.auto.tfvars      # Azure Service Principal credentials (gitignored)
│
├── advanced_tuning.tf.disabled  # Optional: 5 specialized cluster configs
├── streaming.tf.disabled        # Optional: Real-time streaming clusters
├── performance_tuning.tf.disabled # Optional: 5S framework cluster policies
├── apps.tf.disabled             # Optional: Databricks Apps infrastructure
└── lakebase.tf.disabled         # Optional: Lakehouse Federation (Postgres)
```

---

## 🎛️ Configuration

### Environment Variables

Set these in `credentials.auto.tfvars` (file is gitignored):

```hcl
# Azure Service Principal
client_id       = "your-client-id"
client_secret   = "your-client-secret"
tenant_id       = "your-tenant-id"
subscription_id = "your-subscription-id"

# Databricks Workspace (populated after Stage 1)
databricks_host = "https://adb-xxxxx.azuredatabricks.net"

# Optional: Postgres for Lakebase
postgres_host     = "railway-postgres.railway.app"
postgres_port     = "5432"
postgres_database = "railway"
postgres_user     = "postgres"
postgres_password = "your-password"
```

### Feature Flags (`locals.tf`)

Control what gets deployed:

```hcl
locals {
  environments = ["dev"]  # or ["dev", "prod"]

  # Unity Catalog (requires metastore)
  unity_catalog_enabled = false
  metastore_id          = ""  # Set if you have a metastore

  # Cost Control
  deploy_minimal_resources = true  # Disables expensive optional features
}
```

### Cost-Optimized Defaults

The configuration is pre-set for **cost optimization**:
- Only `dev` environment deploys by default
- Expensive features disabled (`.tf.disabled` files)
- Serverless SQL Warehouses (pay per query)
- Single-node clusters where possible
- Auto-termination enabled (10-20 minutes)

---

## 💰 Cost Management

### Monthly Cost Estimates

**Minimal Deployment** (default):
- Storage (ADLS Gen2): ~$50/month
- Monitoring (Log Analytics, App Insights): ~$20/month
- Networking (VNet, NSG): ~$10/month
- Key Vault: ~$5/month
- **Compute (stopped):** $0
- **Total:** ~$85/month base infrastructure

**With Compute Running:**
- SQL Warehouse (2X-Small, Serverless): ~$0.40/hour
- All-Purpose Cluster (Standard_DS3_v2, single-node): ~$0.50/hour
- **4-hour demo session:** ~$2-3

**Full Deployment** (all features enabled):
- Add 9 specialized clusters: +$500-1000/month
- Add streaming clusters: +$200-400/month
- Add cluster policies and apps: +$100-200/month
- **Total:** ~$1,200-1,500/month

---

## 🚀 Deployment Workflows

### Development Environment Only

```bash
# Quick dev deployment
terraform apply -var-file=dev.tfvars -auto-approve

# Outputs workspace URL and storage account name
terraform output databricks_workspace_urls
```

### Enable Optional Features

```bash
# Re-enable disabled modules
mv advanced_tuning.tf.disabled advanced_tuning.tf
mv streaming.tf.disabled streaming.tf

# Deploy with additional features
terraform apply -var-file=dev.tfvars -auto-approve
```

---

## 🔐 Authentication

### Azure CLI (Recommended for Local Development)

```bash
az login
az account set --subscription YOUR_SUBSCRIPTION_ID

# Databricks provider automatically uses Azure CLI tokens
```

---

## 📊 Deployed Resources

### Stage 1: Azure Infrastructure (22 resources)
- ✅ Resource Group, Databricks Workspace
- ✅ ADLS Gen2 with medallion architecture
- ✅ Virtual Network, Subnets, NSG
- ✅ Key Vault, Log Analytics, Application Insights

### Stage 2: Databricks Resources (10 resources)
- ✅ SQL Warehouse (Serverless)
- ✅ All-Purpose Cluster (single-node)
- ✅ User Groups with RBAC
- ✅ Service Principal, Secret Scope

---

## 🧹 Cleanup

```bash
terraform destroy -var-file=dev.tfvars -auto-approve
```

---

## 🔧 Troubleshooting

### Workspace Access Denied

```bash
# Get your Azure AD Object ID
az ad signed-in-user show --query id -o tsv

# Assign Contributor role
az role assignment create \
  --assignee YOUR_OBJECT_ID \
  --role "Contributor" \
  --scope /subscriptions/SUB_ID/resourceGroups/ubereats-dev-rg/providers/Microsoft.Databricks/workspaces/ubereats-dev-workspace
```

---

**Deployed with ❤️ for Semana Databricks 2.0**
