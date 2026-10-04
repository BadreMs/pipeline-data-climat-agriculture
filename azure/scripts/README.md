# Scripts de déploiement Azure

> **Jamais exécutés** : écrits sans abonnement Azure. Les options d'`az` évoluent selon la
> version et les extensions : vérifier chaque commande avec `az <groupe> --help` avant le
> premier `--apply`. Seule la syntaxe shell (`bash -n`) et le mode simulation ont été vérifiés.

## Prérequis

- `az login` fait, et l'identifiant de l'abonnement :
  `az account list -o table` puis `export SUBSCRIPTION_ID=<id>`.
- Azure CLI avec les extensions `datafactory` et `databricks`
  (`az extension add --name datafactory`, `az extension add --name databricks`).
- `jq` et `envsubst` (paquet gettext) pour rendre les JSON Data Factory.
- Droits **Owner**, ou **Contributor** + **User Access Administrator** sur l'abonnement.

## Sécurité

- **Aucun secret ni identifiant en dur** : tout vient de l'environnement
  (`SUBSCRIPTION_ID`, `UNIQUE_SUFFIX`, `SQL_ADMIN_PASSWORD`).
- **Simulation par défaut** : sans `--apply`, les commandes sont seulement affichées, le mot de
  passe apparaît sous la forme `<redacted>`.
- `SQL_ADMIN_PASSWORD` est passé en argument aux commandes `az` en mode `--apply` : il est
  visible dans la liste des processus de la machine pendant l'exécution. À lancer depuis un poste
  de confiance, ou à remplacer par une authentification Entra ID.

## `deploy.sh`

```bash
export SUBSCRIPTION_ID=<id> UNIQUE_SUFFIX=abc123       # 3-8 caractères [a-z0-9]
./deploy.sh                                              # simulation
SQL_ADMIN_PASSWORD=... ./deploy.sh --apply               # exécution réelle
SQL_ADMIN_PASSWORD=... ./deploy.sh --apply --start-triggers
```

| Variable | Défaut | Rôle |
|---|---|---|
| `SUBSCRIPTION_ID` | **obligatoire** | Abonnement cible |
| `UNIQUE_SUFFIX` | **obligatoire** | Suffixe d'unicité globale (stockage, SQL, Key Vault) |
| `SQL_ADMIN_PASSWORD` | obligatoire avec `--apply` | Mot de passe admin du serveur SQL (stocké dans Key Vault) |
| `LOCATION` | `francecentral` | Région Azure |
| `PROJECT`, `ENVIRONMENT` | `climatagri`, `dev` | Préfixes de nommage |
| `RESOURCE_GROUP` | `rg-<project>-<env>` | Nom du resource group |

Ordre : resource group → ADLS Gen2 (conteneurs `bronze`/`silver`/`gold`, fixtures déposées) →
Key Vault → Azure SQL (serverless) → Databricks → Data Factory → rôles de l'identité managée →
ressources ADF (linked services, datasets, pipelines, triggers **arrêtés**). Les étapes
manuelles restantes sont rappelées à la fin de l'exécution.

## `teardown.sh`

Supprime le resource group (et donc **toutes** les données). Simulation par défaut ; avec
`--apply`, il faut retaper le nom du resource group.

```bash
SUBSCRIPTION_ID=<id> ./teardown.sh            # simulation
SUBSCRIPTION_ID=<id> ./teardown.sh --apply    # demande de confirmation
```

Le Key Vault passe en suppression logique : le purger (`az keyvault purge`) pour réutiliser son nom.
