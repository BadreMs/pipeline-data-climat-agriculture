# Déploiement Azure réel — journal

Journal du déploiement effectif de l'extension Azure ([azure/README.md](../azure/README.md)).
Ce document ne rapporte que ce qui a été **exécuté et observé** ; les décisions en attente
sont marquées comme telles. Les coûts réels seront ajoutés au fil des étapes.

**Statut au 2026-10-11 : ingestion ADF (12 régions × 2023-2024), notebooks 01 et 02 sur
Databricks validés de bout en bout. Reste : DDL Azure SQL, notebook 03, pipeline
`transform_databricks` dans ADF, restitution.**

## Contexte

- Abonnement : Azure free account (crédit d'essai). Budget Cost Management de 50 €/mois avec
  alertes (réel 10 % et 40 %, réel 100 %, prévision 100 %). Les alertes **préviennent
  seulement** : elles n'arrêtent aucune ressource, la protection réelle est `teardown.sh`.
- Région : `francecentral`. Groupe de ressources : `rg-climatagri-dev`.
- Les commandes `az` sont lancées à la main, une ressource à la fois, avec validation de la
  sortie avant la suivante. `deploy.sh` n'a pas été utilisé tel quel (voir « Écarts »).
  La partie Data Factory passe par `azure/scripts/deploy_adf_ingest.sh`.

## Ce qui est déployé

| Ressource | État | Remarque |
|---|---|---|
| Resource group `rg-climatagri-dev` | créé | gratuit |
| Storage ADLS Gen2 `stclimatagri<suffix>` | créé | LRS, espace de noms hiérarchique, accès public aux blobs désactivé, conteneurs `bronze`, `silver`, `gold` |
| Key Vault `kv-climatagri-<suffix>` | créé | modèle **Vault access policy** (basculé depuis RBAC : voir « Databricks »), suppression logique ; secrets `sql-user`, `sql-password` (mot de passe généré, jamais affiché ni écrit dans un fichier) et `storage-account-key` |
| Azure SQL (serveur + base `sqldb-climatagri`) | créé | General Purpose serverless, offre « free database » avec `AutoPause` ; authentification SQL conservée, admin Entra ajouté ; règles de pare-feu `AllowAzureServices` et `AllowMyIP` (temporaire) |
| Data Factory `adf-climatagri-<suffix>` | créé | identité managée système, rôle *Storage Blob Data Contributor* sur le Storage |
| Ressources ADF (ingestion) | déployées | 3 services liés, 5 datasets, 2 pipelines, 2 triggers **arrêtés** |
| Ressources `dbg_*` | supprimées | sondes de diagnostic (écho REST et HTTP), retirées après le diagnostic ; dossier `bronze/_debug/` supprimé |
| Databricks workspace (Premium) | créé | cluster interactif `gate-single-node` démarré avec succès sur le Free Trial ; scope secret adossé au Key Vault ; notebooks importés dans `/Shared/pipeline-climat-agriculture` |
| `transform_databricks`, `ls_databricks`, `ls_azuresql` | non déployés dans ADF | dépendent du DDL SQL et de l'utilisateur ADF dans la base |

## Quotas observés

- Fournisseurs nécessaires : `Microsoft.Storage`, `Sql`, `DataFactory`, `Databricks`, `KeyVault`
  déjà enregistrés. `Microsoft.Compute` et `Microsoft.Network` ne l'étaient pas et ont dû être
  enregistrés (gratuit).
- Piège rencontré : tant que `Microsoft.Compute` n'est pas enregistré, `az vm list-usage`
  renvoie une liste **vide** (sans erreur), ce qui ressemble à « aucun quota ».
- Après enregistrement, en `francecentral` : 4 vCPU au total par région, et 4 vCPU pour
  chacune des familles `Standard DSv2`, `DSv3` et `BS`.
- Conséquence : le total régional est partagé par toutes les VM. Un cluster de 4 vCPU en
  fonctionnement empêche de démarrer une autre VM en même temps.

## Azure SQL : offre gratuite

- L'offre « free database » (`--use-free-limit`) est limitée à une base par abonnement.
- Avec `AutoPause` à l'épuisement, **seule la valeur par défaut** du délai de pause est
  acceptée (60 min) : `--auto-pause-delay 15` est refusé (`ProvisioningDisabled`), sans base
  partielle créée.
- Conséquence : à l'épuisement du quota mensuel, la base reste en pause jusqu'au mois suivant,
  mais ne peut pas être facturée.

## Ingestion Open-Meteo : ce qui s'est passé

| Essai | Résultat |
|---|---|
| `RestSource`, en-têtes par défaut | échec côté source après ~11 min (2 retries de 5 min) : `InvalidDataException: Found invalid data while decoding` |
| `RestSource` + `Accept-Encoding: gzip, deflate;q=0` | même échec (~12 min) |
| Sonde REST contre un service d'écho | ADF envoie `Accept-Encoding: gzip, deflate` : **`additionalHeaders` est ignoré** par le connecteur REST |
| Sonde HTTP en mode `Binary` | `additionalHeaders` **honoré** ; octets du serveur écrits sans modification (gzip brut) ; contenu décompressé = JSON valide, `daily.time` sur 7 dates |

Constat serveur (`curl`) : Open-Meteo répond en `deflate` zlib à `gzip, deflate`, en `gzip` si
`deflate` est refusé, en JSON clair avec `identity`.

Décision : connecteur HTTP en mode `Binary` avec `Accept-Encoding: identity`. **À valider** par
un run complet sur une région. Cause du décodage : hypothèse (décodeur `deflate` du connecteur
REST) non démontrée, contournée.

Leçon : un run en échec déterministe coûte ~11 min à cause des retries (2 × 5 min) ; une sonde
`retry: 0` avec un service d'écho est bien plus rapide pour comprendre ce qu'ADF envoie.

## Databricks

**Résultat : un cluster single-node démarre sur le Free Trial.**

- Cluster interactif `gate-single-node` : single node, `Standard_E4ads_v7` (4 vCPU, 32 Go),
  runtime 17.3 LTS (Spark 4.0), Photon désactivé, arrêt automatique à 10 min, 2,06 DBU/h
  affichés. `Standard_DS3_v2` (prévu par `ls_databricks.json`) n'a **jamais été testé**.
- Le quota de 4 vCPU par région est partagé par toutes les VM : un seul cluster à la fois.
- Passer en Pay-as-you-go est **exclu** : il supprime la limite de dépense.

Authentification à ADLS depuis les notebooks (méthode retenue : la plus simple) :

- Les notebooks ne configurent aucun identifiant : la clé du compte de stockage est lue dans
  le Key Vault via un scope secret, et injectée par la configuration Spark du cluster :
  `spark.hadoop.fs.azure.account.key.<compte>.dfs.core.windows.net {{secrets/kv-climat-agriculture/storage-account-key}}`.
- **Les scopes adossés à Key Vault ne supportent que le modèle « Vault access policy »**, pas
  le RBAC (documentation Azure Databricks). Le coffre, créé en RBAC, a dû être basculé, puis une
  politique d'accès a été ajoutée pour l'opérateur. Le nom du scope (`kv-climat-agriculture`)
  est celui que `transform_databricks` attend par défaut.
- La clé de compte donne un accès complet au Storage : acceptable pour cette démonstration
  (groupe de ressources supprimé à la fin), à remplacer par un service principal, un SAS ou
  Unity Catalog en production.
- `spark.sql.ansi.enabled false` est posé sur le cluster : Spark 4 active ANSI par défaut, alors
  que les notebooks ont été écrits pour la sémantique Spark 3 (un `cast` invalide donne `NULL`).

État des notebooks : `01_bronze_to_silver` et `02_silver_to_gold` ont été exécutés avec succès
sur Databricks (données de test : 1 région, 7 jours). `03_gold_to_sql` n'a pas été exécuté : le
DDL Azure SQL n'a pas été joué.

À faire, non encore validé : aligner `ls_databricks.json` (type de nœud, runtime, configuration
Spark) et vérifier que le **job cluster créé par ADF** accepte la clé de compte. La documentation
du linked service n'expose pas le mode d'accès du cluster ; repli prévu : `existingClusterId`.

## Résultats observés : bronze → silver → gold

Run `ingest_openmeteo` sur les 12 régions, 2023-01-01 → 2024-12-31 (1 requête par région) :
`Succeeded`, 12 blobs de 38,2 à 38,7 Ko. L'ancienne partition MA-04 (7 jours) a été conservée :
le silver garde la dernière ingestion par (région, jour).

Notebooks `01_bronze_to_silver` puis `02_silver_to_gold` exécutés sur le cluster interactif :

- `gold/fct_region_climate_kpi` et `gold/fct_solar_potential` : 24 lignes chacun
  (12 régions × 2 ans), `is_complete_year = true` et couvertures à 1,0 partout.
- `precip_et0_ratio` observé de 0,014 (MA-11) à 0,551 (MA-01) ; `solar_potential_score` de 50,4
  (MA-01) à 74,7 (MA-12), avec un gradient nord-sud.
- `has_agriculture_data` vrai en 2023 et faux en 2024 (la fixture s'arrête en 2023) ;
  `spi_simplified` NULL partout (moins de 5 années complètes).

**Parité avec le pipeline local : non vérifiée rigoureusement.** Les ordres de grandeur sont
cohérents, mais aucune comparaison ligne à ligne (mêmes régions, mêmes années) n'a été faite.
Un test exact reste à faire avant d'affirmer que les deux piles donnent les mêmes résultats.

## Écarts constatés dans `azure/scripts/deploy.sh`

Constatés à la relecture, à corriger sur la branche `feat/azure-deploy` :

1. Key Vault en autorisation RBAC : le propriétaire de l'abonnement n'a pas les droits sur les
   secrets. Il faut d'abord s'attribuer *Key Vault Secrets Officer*. *(Contourné à la main.)*
2. Même chose pour le stockage : *Storage Blob Data Contributor* est nécessaire pour créer les
   conteneurs et téléverser avec `--auth-mode login`. *(Contourné à la main.)*
3. Aucun administrateur Entra n'est défini sur le serveur SQL, ce qui bloque
   `CREATE USER … FROM EXTERNAL PROVIDER` pour l'identité managée d'ADF. *(Admin ajouté ; l'utilisateur
   ADF n'est utile qu'avec `transform_databricks`, pas encore créé.)*
4. Le pare-feu SQL n'autorise pas l'IP de l'opérateur, donc pas de DDL depuis le poste.
   *(Règle temporaire ajoutée.)*
5. Le mot de passe SQL passe en argument de ligne de commande. *(Généré dans une variable du
   shell et stocké dans Key Vault ; il transite brièvement dans les arguments du processus `az`.)*
6. Le nom `kv-climat-agriculture` (paramètre de `transform_databricks`) diffère du nom du
   Key Vault créé par le script. *(Résolu : c'est le nom du scope Databricks, créé tel quel.)*
7. Databricks Premium et Unity Catalog configurés à la main : à simplifier. *(Simplifié : clé de
   compte via scope Key Vault, sans Unity Catalog.)*
8. Le déploiement ADF envoie tous les dossiers : sans Databricks, `ls_databricks`,
   `transform_databricks` et `tr_daily_transform` doivent être exclus. *(Traité par
   `deploy_adf_ingest.sh`.)*
9. L'ingestion REST échoue (voir ci-dessus) : remplacée par le connecteur HTTP en mode binaire.
10. `deploy.sh` crée le Key Vault en RBAC, incompatible avec un scope secret Databricks adossé à
    Key Vault : à créer en « Vault access policy » ou à utiliser un scope Databricks-backed.
11. `ls_databricks.json` cible `Standard_DS3_v2` et le runtime 15.4, alors que le cluster validé
    est `Standard_E4ads_v7` en 17.3 LTS.

## Défaut de conception connu : partitions bronze sans plage de dates

Le sink `ds_bronze_openmeteo_json` écrit à
`openmeteo/ingest_date=<jour>/region_code=<code>/data.json`. Le chemin ne contient **pas** la
plage `startDate`–`endDate` : deux runs le **même jour** sur des plages différentes écrasent le
fichier de chaque région. Le conseil du README des pipelines (backfill par tranches de 2 ans)
ne fonctionnerait donc pas en un seul jour : seule la dernière tranche serait conservée.

Contournement actuel : un seul run par jour, avec une plage couvrant tout le besoin (2023-01-01 →
2024-12-31 pour l'enrichissement de démonstration, 2 ans par requête comme le client local).

Correction envisagée, **non faite** : inclure `startDate` dans le nom de fichier ou le dossier
et adapter le motif de lecture du notebook 01 (`data.json` → `data*.json`).

Autre limite connue : la fixture agricole couvre 2015-2023, la météo ingérée 2023-2024 : les
colonnes agricoles de 2024 sont NULL (jointure à gauche), et `spi_simplified` reste NULL tant
qu'il y a moins de 5 années complètes de référence.

## Coûts réels observés

À compléter à la fin de chaque étape (Cost Management, filtré sur le groupe de ressources).

| Étape | Estimé | Observé |
|---|---|---|
| Storage | < 1 €/mois | à relever |
| Key Vault | < 1 €/mois | à relever |
| Azure SQL | 0 à 12 €/mois | à relever |
| Data Factory | 1 à 8 €/mois | à relever (2 runs REST en échec de 11 et 12 min, sondes `dbg_*`) |
| Databricks (test) | à estimer avant création | à relever |

## Nettoyage

Fin de démonstration : `azure/scripts/teardown.sh --apply` supprime le groupe de ressources.
Le Key Vault est supprimé de façon logique : son nom reste réservé tant qu'il n'est pas purgé
(`az keyvault purge`). Avant cela : supprimer la règle de pare-feu `AllowMyIP` si le groupe de
ressources est conservé, et terminer le cluster Databricks s'il est actif.
