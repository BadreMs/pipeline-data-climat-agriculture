#!/usr/bin/env bash
# Deploie dans Data Factory SEULEMENT la chaine d'ingestion (ingest_openmeteo, ingest_agriculture),
# sans les ressources qui dependent de Databricks ou d'Azure SQL (ls_databricks, ls_azuresql,
# transform_databricks, tr_daily_transform). Les ressources de l'ADF et du stockage doivent deja
# exister (voir docs/azure_actual_deployment.md) ; ce script n'en cree aucune.
#
# MODE SIMULATION PAR DEFAUT : sans --apply, les commandes sont seulement affichees.
# Les triggers sont crees ARRETES et jamais demarres par ce script.
#
# Usage :
#   UNIQUE_SUFFIX=abc123 ./deploy_adf_ingest.sh            # simulation
#   UNIQUE_SUFFIX=abc123 ./deploy_adf_ingest.sh --apply
#
# Prerequis : az (extension datafactory), envsubst, python (aucun jq requis). Le compte az courant
# est utilise tel quel : ce script ne change jamais d'abonnement.
set -euo pipefail

APPLY=false
for arg in "$@"; do
    case "$arg" in
        --apply) APPLY=true ;;
        -h | --help)
            sed -n '2,15p' "$0"
            exit 0
            ;;
        *)
            echo "Option inconnue : $arg (voir --help)" >&2
            exit 2
            ;;
    esac
done

: "${UNIQUE_SUFFIX:?Definir UNIQUE_SUFFIX : suffixe du stockage et de l ADF}"

PROJECT="${PROJECT:-climatagri}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
RESOURCE_GROUP="${RESOURCE_GROUP:-rg-${PROJECT}-${ENVIRONMENT}}"
STORAGE_ACCOUNT_NAME="${STORAGE_ACCOUNT_NAME:-st${PROJECT}${UNIQUE_SUFFIX}}"
ADF_NAME="${ADF_NAME:-adf-${PROJECT}-${UNIQUE_SUFFIX}}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ADF_DIR="${REPO_ROOT}/azure/data_factory"

# Liste explicite, dans l'ordre de dependance : un nouveau fichier n'est jamais deploye par accident.
LINKED_SERVICES=(ls_adls_datalake ls_http_openmeteo ls_http_datagovma)
DATASETS=(ds_openmeteo_archive ds_bronze_openmeteo_json ds_http_agriculture_csv
    ds_bronze_agriculture_csv ds_bronze_seed_csv)
PIPELINES=(ingest_openmeteo ingest_agriculture)
TRIGGERS=(tr_daily_openmeteo tr_monthly_agriculture)

# Seul ${STORAGE_ACCOUNT_NAME} figure dans ces ressources.
SUBSTITUTIONS='${STORAGE_ACCOUNT_NAME}'
export STORAGE_ACCOUNT_NAME

# Premier interpreteur capable d'importer json. Ecarte le stub python3 du Microsoft Store
# (Windows), que `command -v` trouve mais qui n'execute pas Python.
PYTHON=""
for candidate in python python3 py; do
    if command -v "$candidate" >/dev/null 2>&1 && "$candidate" -c 'import json' >/dev/null 2>&1; then
        PYTHON="$(command -v "$candidate")"
        break
    fi
done

# Dossier temporaire des JSON rendus, supprime a la sortie meme en cas d'erreur.
WORK_DIR=""
cleanup() { [ -z "$WORK_DIR" ] || rm -rf "$WORK_DIR"; }
trap cleanup EXIT

if [ "$APPLY" = true ]; then
    [ -n "$PYTHON" ] || { echo "python introuvable ou non fonctionnel sur le PATH." >&2; exit 1; }
    WORK_DIR="$(mktemp -d)"
fi

# `az` est un programme Windows sous Git Bash : il lui faut un chemin Windows pour @fichier.
native_path() {
    if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s' "$1"; fi
}

step() { printf '\n== %s\n' "$*"; }

deploy_group() {
    local folder="$1" az_group="$2" option="$3"
    shift 3
    local name file rendered
    for name in "$@"; do
        file="${ADF_DIR}/${folder}/${name}.json"
        [ -f "$file" ] || { echo "Fichier introuvable : $file" >&2; exit 1; }
        if [ "$APPLY" = true ]; then
            # Format Git d'ADF {name, properties} : az attend uniquement `properties`.
            rendered="${WORK_DIR}/${name}.json"
            envsubst "$SUBSTITUTIONS" <"$file" |
                "$PYTHON" -c 'import json, sys; print(json.dumps(json.load(sys.stdin)["properties"]))' \
                    >"$rendered"
            az datafactory "$az_group" create --factory-name "$ADF_NAME" \
                --resource-group "$RESOURCE_GROUP" --name "$name" \
                "$option" "@$(native_path "$rendered")" --query name -o tsv
        else
            printf '[simulation] az datafactory %s create --factory-name %s --name %s %s @<%s rendu>\n' \
                "$az_group" "$ADF_NAME" "$name" "$option" "${name}.json"
        fi
    done
}

step "Data Factory : ${ADF_NAME} (groupe ${RESOURCE_GROUP}, stockage ${STORAGE_ACCOUNT_NAME})"
deploy_group linked_services linked-service --properties "${LINKED_SERVICES[@]}"
deploy_group datasets dataset --properties "${DATASETS[@]}"
deploy_group pipelines pipeline --pipeline "${PIPELINES[@]}"
deploy_group triggers trigger --properties "${TRIGGERS[@]}"

echo
echo "Triggers crees a l'etat arrete. Aucun n'est demarre par ce script."
echo "Non deployes (dependent de Databricks / Azure SQL) : ls_databricks, ls_azuresql,"
echo "transform_databricks, tr_daily_transform."
