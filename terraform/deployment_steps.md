# Step-by-Step Deployment Guide
## Databricks on Azure - Two-Stage Terraform Deployment

---

## 📋 Pre-Deployment Checklist

### 1. Verify Prerequisites

```bash
# Check Terraform version (needs >= 1.5.0)
terraform --version

# Check Azure CLI is installed and authenticated
az --version
az account show

# Verify you're on the correct subscription
az account list --output table
```

### 2. Prepare Credentials File

Create `terraform/credentials.auto.tfvars` with your Azure Service Principal:

```hcl
# Azure Service Principal Credentials
client_id       = "your-azure-sp-client-id"
client_secret   = "your-azure-sp-client-secret"
tenant_id       = "your-azure-tenant-id"
subscription_id = "your-azure-subscription-id"

# Databricks Workspace URL (leave empty, will be populated after Stage 1)
databricks_host = ""

# Optional: Postgres for Lakebase (if using)
postgres_host     = ""
postgres_port     = "5432"
postgres_database = ""
postgres_user     = ""
postgres_password = ""
```

**Important:** This file is gitignored for security.

### 3. Review Configuration

Check `terraform/locals.tf` for cost control settings:

```hcl
locals {
  environments = ["dev"]  # Only dev to control costs

  unity_catalog_enabled = false  # Set to true if you have a metastore
  metastore_id          = ""     # Your metastore ID if enabled
}
```

---

## 🚀 Stage 1: Deploy Azure Infrastructure

### Step 1: Navigate to Terraform Directory

```bash
cd /Users/luanmorenomaciel/GitHub/semana-databricks-2-0/terraform
```

### Step 2: Initialize Terraform

```bash
terraform init
```

**What this does:**
- Downloads required provider plugins (Azure, Databricks, Random)
- Creates `.terraform/` directory
- Generates `.terraform.lock.hcl` lock file

**Expected output:**
```
Terraform has been successfully initialized!
```

### Step 3: Review Deployment Plan

```bash
terraform plan -var-file=dev.tfvars
```

**What to look for:**
- Number of resources to be created (~32 resources)
- Resource types (should see azurerm_* and databricks_* resources)
- No errors or warnings
- Storage account names (will have random suffix)

**★ Insight ─────────────────────────────────────**
**Stage 1 Resources to be Created:**
- Resource Group: `ubereats-dev-rg`
- Databricks Workspace: `ubereats-dev-workspace` (3-5 min creation time)
- ADLS Gen2 Storage: `adlsubereatsdev<random>` with 4 filesystems
- Virtual Network: `ubereats-dev-vnet` with public/private subnets
- Network Security Group: `ubereats-dev-nsg`
- Key Vault: `ubereats-dev-kv` (has soft-delete, takes 5-10 min to destroy)
- Log Analytics Workspace: `ubereats-dev-law`
- Application Insights: 2 instances (general + ML)
- Monitor Action Group: For alerting

**Stage 2 Resources to be Created (if workspace auth succeeds):**
- Databricks SQL Warehouse: Serverless 2X-Small (auto-stops)
- All-Purpose Cluster: Standard_DS3_v2 single-node (not started)
- User Groups: data-analysts, data-engineers, data-scientists, ml-engineers
- Service Principal: For automation
- Secret Scope: For credentials
- RBAC Permissions: Catalog and resource-level grants
─────────────────────────────────────────────────

### Step 4: Deploy Infrastructure

```bash
terraform apply -var-file=dev.tfvars -auto-approve
```

**Timeline:**
- **0-2 minutes:** Azure infrastructure provisioning
  - Resource group created immediately
  - Networking resources (VNet, subnets, NSG) created
  - Storage accounts start provisioning

- **2-5 minutes:** Databricks workspace provisioning
  - Workspace creation (longest step)
  - Managed resource group created automatically
  - Workspace URL becomes available

- **5-7 minutes:** Databricks resources provisioning (if auth works)
  - User groups created
  - Service principal created
  - SQL warehouse created (stopped state)
  - All-purpose cluster created (not started)

**Watch for:**
- Green `Creation complete` messages
- Workspace URL in output
- Any authentication errors for Databricks resources

### Step 5: Capture Workspace URL

```bash
terraform output databricks_workspace_urls
```

**Example output:**
```json
{
  "dev": "adb-519425053193597.17.azuredatabricks.net"
}
```

**Copy this URL!** You'll need it for workspace access.

### Step 6: Update dev.tfvars with Workspace URL

If Databricks resources failed to deploy (authentication issue), update:

```hcl
# In dev.tfvars, add:
databricks_host = "https://adb-519425053193597.17.azuredatabricks.net"
```

Then re-run:
```bash
terraform apply -var-file=dev.tfvars -auto-approve
```

---

## 🔐 Stage 2: Configure Workspace Access

### Step 7: Grant Yourself Workspace Access

```bash
# Get your Azure AD Object ID
OBJECT_ID=$(az ad signed-in-user show --query id -o tsv)
echo "Your Object ID: $OBJECT_ID"

# Get workspace resource ID
WORKSPACE_ID=$(az databricks workspace show \
  --resource-group ubereats-dev-rg \
  --name ubereats-dev-workspace \
  --query id -o tsv)
echo "Workspace ID: $WORKSPACE_ID"

# Assign Contributor role
az role assignment create \
  --assignee $OBJECT_ID \
  --role "Contributor" \
  --scope $WORKSPACE_ID
```

**Wait 5-10 minutes** for Azure role propagation.

### Step 8: Access Databricks Workspace

```bash
# Get workspace URL
WORKSPACE_URL=$(terraform output -json databricks_workspace_urls | jq -r '.dev')
echo "Workspace URL: https://$WORKSPACE_URL"

# Open in browser
open "https://$WORKSPACE_URL"
```

**Login with your Azure credentials.**

If you see "Access Denied":
- Wait 5-10 more minutes for role propagation
- Clear browser cache and try again
- Try incognito/private browsing mode

---

## 📊 Verify Deployment

### Step 9: Check Deployed Resources

**In Azure Portal:**
```bash
# Open resource group in portal
az group show --name ubereats-dev-rg --query id -o tsv
```

Navigate to: https://portal.azure.com → Resource Groups → `ubereats-dev-rg`

**Verify you see:**
- Databricks Workspace
- Storage Account (adlsubereatsdev<suffix>)
- Virtual Network
- Key Vault
- Log Analytics Workspace
- Application Insights (2 instances)
- Network Security Group

**In Databricks Workspace:**

1. **Catalog Section** (left sidebar)
   - Navigate to Data → Catalog
   - Should see `hive_metastore` (default)
   - If Unity Catalog enabled: should see `dev_ubereats_delivery_services`

2. **Compute Section** (left sidebar)
   - Navigate to Compute
   - Should see:
     - `ubereats-dev-job-cluster` (not started, single-node)
   - Note: Cluster is configured but NOT running (cost = $0)

3. **SQL Warehouses** (left sidebar)
   - Navigate to SQL → Warehouses
   - Should see:
     - `ubereats-dev-sql-warehouse` (Serverless, 2X-Small, stopped)
   - Note: Warehouse is stopped (cost = $0)

4. **User Groups** (Settings → Admin Console → Groups)
   - Should see:
     - data-analysts
     - data-engineers
     - data-scientists
     - ml-engineers

### Step 10: Check Terraform State

```bash
# List all managed resources
terraform state list

# Should show ~32 resources like:
# - azurerm_resource_group.this["dev"]
# - azurerm_databricks_workspace.this["dev"]
# - azurerm_storage_account.adls["dev"]
# - databricks_sql_endpoint.this["dev"]
# - databricks_cluster.job_cluster["dev"]
# etc.
```

---

## 💰 Cost Verification

### Step 11: Verify Cost Controls

**Check that expensive resources are NOT running:**

```bash
# Verify clusters are not started
# (should be empty or show "TERMINATED" status)
# You'd need to use Databricks CLI or UI for this

# Verify disabled modules are still disabled
ls -la *.tf.disabled

# Should see:
# - advanced_tuning.tf.disabled (5 specialized clusters)
# - streaming.tf.disabled (2 streaming clusters)
# - performance_tuning.tf.disabled (8 cluster policies)
# - apps.tf.disabled (Databricks Apps)
# - lakebase.tf.disabled (Postgres federation)
```

**Current cost:**
- Base infrastructure: ~$85/month (storage, networking, monitoring)
- Compute: $0 (all stopped/not started)
- **Total: ~$85/month**

---

## 🧪 Optional: Test Infrastructure

### Step 12: Test SQL Warehouse (Optional)

**In Databricks Workspace:**

1. Navigate to SQL → Warehouses
2. Find `ubereats-dev-sql-warehouse`
3. Click **Start** (takes ~1 minute)
4. Create a new SQL query
5. Run test query:
   ```sql
   SELECT current_user(), current_database();
   ```
6. **Stop warehouse immediately after test** (auto-stops after 10 min anyway)

**Cost impact:** ~$0.40/hour while running

### Step 13: Test Cluster Creation (Optional)

**In Databricks Workspace:**

1. Navigate to Compute
2. Find `ubereats-dev-job-cluster`
3. Click **Start** (takes ~3-5 minutes)
4. Create a notebook
5. Run test code:
   ```python
   print(f"Spark version: {spark.version}")
   print(f"Available cores: {sc.defaultParallelism}")
   ```
6. **Terminate cluster immediately after test**

**Cost impact:** ~$0.50/hour while running

---

## 🧹 Cleanup (When Done)

### Step 14: Destroy All Resources

**⚠️ WARNING: This deletes EVERYTHING. All data will be lost.**

```bash
# Full destruction
terraform destroy -var-file=dev.tfvars -auto-approve
```

**Timeline:**
- **0-2 minutes:** Databricks resources deleted
  - Clusters, SQL warehouses terminated
  - User groups, service principals removed
  - Secret scopes deleted

- **2-8 minutes:** Azure resources deleted
  - Storage accounts purged (data permanently deleted)
  - Databricks workspace destroyed
  - Networking resources removed

- **8-10 minutes:** Final cleanup
  - Key Vault deleted (soft-delete, retains 7 days)
  - Log Analytics workspace deleted
  - Resource group removed

**Verify complete destruction:**

```bash
# Should return error: ResourceGroupNotFound
az group show --name ubereats-dev-rg

# Should show empty state
terraform state list
```

**Cost after destruction:** $0/month

### Step 15: Purge Key Vault (Optional)

Key Vault has soft-delete protection. To permanently delete:

```bash
az keyvault purge \
  --name ubereats-dev-kv \
  --location eastus2
```

---

## 🔄 Re-Deployment

To deploy again later:

```bash
# Clean slate
rm -rf .terraform/ terraform.tfstate*

# Re-initialize
terraform init

# Deploy fresh
terraform apply -var-file=dev.tfvars -auto-approve
```

---

## 📝 Quick Reference Commands

### Check Status
```bash
terraform state list                    # List all resources
terraform output                        # Show all outputs
terraform show                          # Show current state
```

### Targeted Operations
```bash
# Deploy only Azure infrastructure
terraform apply -target='azurerm_resource_group.this["dev"]' -var-file=dev.tfvars

# Destroy only Databricks resources
terraform destroy -target='databricks_sql_endpoint.this["dev"]' -var-file=dev.tfvars
```

### Troubleshooting
```bash
# Refresh state
terraform refresh -var-file=dev.tfvars

# Validate configuration
terraform validate

# Format Terraform files
terraform fmt -recursive
```

---

## ⚠️ Important Notes

1. **Two-Stage Dependency:**
   - Stage 1 creates workspace
   - Stage 2 needs workspace URL and authentication
   - If Stage 2 fails, re-run after fixing auth

2. **Authentication:**
   - Azure CLI auth: Automatic, recommended for local development
   - Service Principal auth: Configure in providers.tf for CI/CD

3. **Cost Control:**
   - Keep expensive features disabled (.tf.disabled files)
   - Start compute only when needed
   - Use auto-termination (already configured)
   - Deploy dev environment only

4. **Git Safety:**
   - `.gitignore` protects `credentials.auto.tfvars`
   - Never commit `terraform.tfstate` (contains sensitive data)
   - Never commit `*.log` files

5. **Resource Group:**
   - Contains all resources
   - Deleting it removes everything
   - Managed resource group created automatically by Databricks

---

## 🎯 Success Criteria

Deployment is successful when:

- ✅ `terraform apply` completes without errors
- ✅ Workspace URL accessible in browser
- ✅ Can see 4 user groups in workspace
- ✅ SQL warehouse appears in Warehouses section (stopped)
- ✅ All-purpose cluster appears in Compute section (not started)
- ✅ Storage account has 4 filesystems (landing, bronze, silver, gold)
- ✅ Cost is ~$85/month (compute not running)
- ✅ All resources appear in Azure Portal under `ubereats-dev-rg`

---

**Ready to execute? Follow these steps in order and you'll have a fully deployed, cost-optimized Databricks environment! 🚀**
