#!/usr/bin/env bash
# Squelette de provisionnement Azure du pipeline medallion (ADLS Gen2, Data Factory,
# Databricks, Azure SQL DB, Key Vault).
#
# NON EXECUTE a ce jour : ecrit sans acces a un abonnement Azure, jamais teste. Verifier chaque
# commande avec `az <groupe> --help` (les options evoluent selon la version d'az et des
# extensions) avant le premier --apply.
#
# MODE SIMULATION PAR DEFAUT : sans --apply, les commandes sont seulement affichees.
#
# Usage :
#   SUBSCRIPTION_ID=... UNIQUE_SUFFIX=abc123 ./deploy.sh              # simulation
#   SUBSCRIPTION_ID=... UNIQUE_SUFFIX=abc123 SQL_ADMIN_PASSWORD=... ./deploy.sh --apply
#
# Aucun secret dans ce fichier : tout vient de l'environnement.
set -euo pipefail

APPLY=false
START_TRIGGERS=false
for arg in "$@"; do
    case "$arg" in
        --apply) APPLY=true ;;
        --start-triggers) START_TRIGGERS=true ;;
        -h | --help)
            sed -n '2,17p' "$0"
            exit 0
            ;;
        *)
            echo "Option inconnue : $arg (voir --help)" >&2
            exit 2
            ;;
    esac
done

# --- Variables ----------------------------------------------------------------------------
: "${SUBSCRIPTION_ID:?Definir SUBSCRIPTION_ID (az account list -o table)}"
: "${UNIQUE_SUFFIX:?Definir UNIQUE_SUFFIX : 3 a 8 caracteres [a-z0-9], unicite des noms globaux}"
if [ "$APPLY" = true ]; then
    : "${SQL_ADMIN_PASSWORD:?Definir SQL_ADMIN_PASSWORD (jamais en dur, jamais commite)}"
else
    SQL_ADMIN_PASSWORD="<redacted>"
fi

LOCATION="${LOCATION:-francecentral}"
PROJECT="${PROJECT:-climatagri}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
RESOURCE_GROUP="${RESOURCE_GROUP:-rg-${PROJECT}-${ENVIRONMENT}}"
STORAGE_ACCOUNT_NAME="st${PROJECT}${UNIQUE_SUFFIX}"   # 3-24 car., minuscules/chiffres
KEY_VAULT_NAME="kv-${PROJECT}-${UNIQUE_SUFFIX}"
ADF_NAME="adf-${PROJECT}-${UNIQUE_SUFFIX}"
DATABRICKS_WORKSPACE_NAME="dbw-${PROJECT}-${UNIQUE_SUFFIX}"
SQL_SERVER_NAME="sql-${PROJECT}-${UNIQUE_SUFFIX}"
SQL_DATABASE_NAME="sqldb-${PROJECT}"
SQL_ADMIN_USER="${SQL_ADMIN_USER:-sqladmin${PROJECT}}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ADF_DIR="${REPO_ROOT}/azure/data_factory"

run() {
    if [ "$APPLY" = true ]; then
        "$@"
    else
        printf '[simulation]'
        printf ' %q' "$@"
        printf '\n'
    fi
}

step() { printf '\n== %s\n' "$*"; }

# --- 1. Abonnement et groupe de ressources ------------------------------------------------
step "Abonnement et resource group"
run az account set --subscription "$SUBSCRIPTION_ID"
run az group create --name "$RESOURCE_GROUP" --location "$LOCATION" \
    --tags project=pipeline-climat-agriculture environment="$ENVIRONMENT"

# --- 2. Data lake ADLS Gen2 (namespace hierarchique) et conteneurs medallion ---------------
step "ADLS Gen2 : $STORAGE_ACCOUNT_NAME"
run az storage account create --name "$STORAGE_ACCOUNT_NAME" --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" --sku Standard_LRS --kind StorageV2 --enable-hierarchical-namespace true \
    --allow-blob-public-access false --min-tls-version TLS1_2
for container in bronze silver gold; do
    run az storage container create --name "$container" --account-name "$STORAGE_ACCOUNT_NAME" \
        --auth-mode login
done

# Donnees de reference lues par le pipeline : fixture agriculture (mock) et dim_region.
run az storage blob upload --account-name "$STORAGE_ACCOUNT_NAME" --auth-mode login \
    --container-name bronze --name seed/agriculture_maroc_mock.csv --overwrite \
    --file "${REPO_ROOT}/ingestion/fixtures/agriculture_maroc_mock.csv"
run az storage blob upload --account-name "$STORAGE_ACCOUNT_NAME" --auth-mode login \
    --container-name bronze --name reference/dim_region.csv --overwrite \
    --file "${REPO_ROOT}/dbt_project/seeds/dim_region.csv"

# --- 3. Key Vault (secrets SQL consommes par le scope Databricks) --------------------------
step "Key Vault : $KEY_VAULT_NAME"
run az keyvault create --name "$KEY_VAULT_NAME" --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" --enable-rbac-authorization true
run az keyvault secret set --vault-name "$KEY_VAULT_NAME" --name sql-user --value "$SQL_ADMIN_USER"
run az keyvault secret set --vault-name "$KEY_VAULT_NAME" --name sql-password \
    --value "$SQL_ADMIN_PASSWORD"

# --- 4. Azure SQL Database (serverless, mise en veille automatique) -------------------------
step "Azure SQL : $SQL_SERVER_NAME / $SQL_DATABASE_NAME"
run az sql server create --name "$SQL_SERVER_NAME" --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" --admin-user "$SQL_ADMIN_USER" --admin-password "$SQL_ADMIN_PASSWORD"
run az sql server firewall-rule create --server "$SQL_SERVER_NAME" --resource-group "$RESOURCE_GROUP" \
    --name AllowAzureServices --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0
run az sql db create --name "$SQL_DATABASE_NAME" --server "$SQL_SERVER_NAME" \
    --resource-group "$RESOURCE_GROUP" --edition GeneralPurpose --family Gen5 \
    --compute-model Serverless --capacity 1 --min-capacity 0.5 --auto-pause-delay 60 \
    --backup-storage-redundancy Local

# --- 5. Databricks et Data Factory ---------------------------------------------------------
step "Databricks : $DATABRICKS_WORKSPACE_NAME"
run az databricks workspace create --name "$DATABRICKS_WORKSPACE_NAME" \
    --resource-group "$RESOURCE_GROUP" --location "$LOCATION" --sku premium

step "Data Factory : $ADF_NAME"
run az datafactory create --factory-name "$ADF_NAME" --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION"

# En simulation, les identifiants ci-dessous n'existent pas encore : valeurs fictives.
if [ "$APPLY" = true ]; then
    ADF_PRINCIPAL_ID="$(az datafactory show --factory-name "$ADF_NAME" \
        --resource-group "$RESOURCE_GROUP" --query identity.principalId -o tsv)"
    STORAGE_ID="$(az storage account show --name "$STORAGE_ACCOUNT_NAME" \
        --resource-group "$RESOURCE_GROUP" --query id -o tsv)"
    DATABRICKS_WORKSPACE_RESOURCE_ID="$(az databricks workspace show \
        --name "$DATABRICKS_WORKSPACE_NAME" --resource-group "$RESOURCE_GROUP" --query id -o tsv)"
    DATABRICKS_WORKSPACE_URL="$(az databricks workspace show \
        --name "$DATABRICKS_WORKSPACE_NAME" --resource-group "$RESOURCE_GROUP" \
        --query workspaceUrl -o tsv)"
else
    ADF_PRINCIPAL_ID="<adf-principal-id>"
    STORAGE_ID="<storage-resource-id>"
    DATABRICKS_WORKSPACE_RESOURCE_ID="<databricks-workspace-resource-id>"
    DATABRICKS_WORKSPACE_URL="<databricks-workspace-url>"
fi

step "Roles : l'identite managee d'ADF ecrit dans le lake et pilote Databricks"
run az role assignment create --assignee-object-id "$ADF_PRINCIPAL_ID" \
    --assignee-principal-type ServicePrincipal --role "Storage Blob Data Contributor" \
    --scope "$STORAGE_ID"
run az role assignment create --assignee-object-id "$ADF_PRINCIPAL_ID" \
    --assignee-principal-type ServicePrincipal --role Contributor \
    --scope "$DATABRICKS_WORKSPACE_RESOURCE_ID"

# --- 6. Ressources Data Factory (JSON du repo, placeholders ${...} remplaces) ---------------
step "Ressources Data Factory (linked services -> datasets -> pipelines -> triggers)"
export STORAGE_ACCOUNT_NAME SQL_SERVER_NAME SQL_DATABASE_NAME \
    DATABRICKS_WORKSPACE_URL DATABRICKS_WORKSPACE_RESOURCE_ID
SUBSTITUTIONS='${STORAGE_ACCOUNT_NAME} ${SQL_SERVER_NAME} ${SQL_DATABASE_NAME} ${DATABRICKS_WORKSPACE_URL} ${DATABRICKS_WORKSPACE_RESOURCE_ID}'

deploy_adf_resource() {
    local folder="$1" az_group="$2" option="$3" file name rendered
    for file in "${ADF_DIR}/${folder}"/*.json; do
        name="$(basename "$file" .json)"
        if [ "$APPLY" = true ]; then
            # Les fichiers sont au format Git d'ADF {name, properties} : az attend `properties`.
            rendered="$(mktemp)"
            envsubst "$SUBSTITUTIONS" <"$file" | jq '.properties' >"$rendered"
            az datafactory "$az_group" create --factory-name "$ADF_NAME" \
                --resource-group "$RESOURCE_GROUP" --name "$name" "$option" "@${rendered}"
            rm -f "$rendered"
        else
            printf '[simulation] az datafactory %s create --name %s %s @<%s rendu>\n' \
                "$az_group" "$name" "$option" "$(basename "$file")"
        fi
    done
}

deploy_adf_resource linked_services linked-service --properties
deploy_adf_resource datasets dataset --properties
deploy_adf_resource pipelines pipeline --pipeline
deploy_adf_resource triggers trigger --properties

if [ "$START_TRIGGERS" = true ]; then
    for trigger in tr_daily_openmeteo tr_monthly_agriculture tr_daily_transform; do
        run az datafactory trigger start --factory-name "$ADF_NAME" \
            --resource-group "$RESOURCE_GROUP" --name "$trigger"
    done
else
    echo "Triggers crees a l'etat arrete (couts maitrises). Relancer avec --start-triggers pour les demarrer."
fi

# --- 7. Etapes manuelles restantes ---------------------------------------------------------
cat <<EOF

== Etapes manuelles restantes (non couvertes par az cli) ==
1. Databricks : importer azure/databricks/notebooks/*.py dans /Shared/pipeline-climat-agriculture
   (databricks CLI : databricks workspace import-dir ...).
2. Databricks : creer le secret scope adosse a Key Vault "${KEY_VAULT_NAME}" (nom attendu par
   defaut : kv-climat-agriculture) avec les secrets sql-user et sql-password.
3. Databricks / Unity Catalog : external locations sur abfss://{bronze,silver,gold}@${STORAGE_ACCOUNT_NAME}
   et droits READ/WRITE pour le job cluster.
4. Azure SQL : executer azure/sql/01_create_schemas.sql puis 02_create_serving_tables.sql, puis
   CREATE USER [${ADF_NAME}] FROM EXTERNAL PROVIDER + droits EXECUTE sur marts.usp_refresh_serving.
5. Premier test : declencher ingest_openmeteo avec startDate=2015-01-01 (backfill), puis
   transform_databricks.
EOF
