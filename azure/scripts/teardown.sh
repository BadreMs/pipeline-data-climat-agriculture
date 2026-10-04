#!/usr/bin/env bash
# Supprime le resource group du pipeline Azure (ADLS, Data Factory, Databricks, Azure SQL,
# Key Vault) pour stopper toute facturation.
#
# NON EXECUTE a ce jour (aucun abonnement Azure n'a ete utilise). Operation DESTRUCTIVE et
# irreversible : donnees du lake et base SQL comprises.
#
# MODE SIMULATION PAR DEFAUT : sans --apply, la commande est seulement affichee. Avec --apply,
# il faut en plus retaper le nom du resource group pour confirmer.
#
# Usage :
#   SUBSCRIPTION_ID=... ./teardown.sh
#   SUBSCRIPTION_ID=... ./teardown.sh --apply
set -euo pipefail

APPLY=false
for arg in "$@"; do
    case "$arg" in
        --apply) APPLY=true ;;
        -h | --help)
            sed -n '2,13p' "$0"
            exit 0
            ;;
        *)
            echo "Option inconnue : $arg (voir --help)" >&2
            exit 2
            ;;
    esac
done

: "${SUBSCRIPTION_ID:?Definir SUBSCRIPTION_ID (az account list -o table)}"

PROJECT="${PROJECT:-climatagri}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
RESOURCE_GROUP="${RESOURCE_GROUP:-rg-${PROJECT}-${ENVIRONMENT}}"
KEY_VAULT_NAME="${KEY_VAULT_NAME:-}"

if [ "$APPLY" != true ]; then
    echo "[simulation] az account set --subscription ${SUBSCRIPTION_ID}"
    echo "[simulation] az group delete --name ${RESOURCE_GROUP} --yes --no-wait"
    echo "Relancer avec --apply pour supprimer reellement (confirmation demandee)."
    exit 0
fi

read -r -p "Supprimer DEFINITIVEMENT le resource group '${RESOURCE_GROUP}' ? Retaper son nom : " answer
if [ "$answer" != "$RESOURCE_GROUP" ]; then
    echo "Nom different : suppression annulee." >&2
    exit 1
fi

az account set --subscription "$SUBSCRIPTION_ID"
az group delete --name "$RESOURCE_GROUP" --yes --no-wait
echo "Suppression lancee en arriere-plan : az group show --name ${RESOURCE_GROUP} pour suivre."

if [ -n "$KEY_VAULT_NAME" ]; then
    echo "Key Vault en suppression logique : une fois le groupe supprime, liberer le nom avec"
    echo "  az keyvault purge --name ${KEY_VAULT_NAME}"
fi
