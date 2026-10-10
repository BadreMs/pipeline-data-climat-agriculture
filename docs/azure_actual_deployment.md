# Déploiement Azure réel — journal

Journal du déploiement effectif de l'extension Azure ([azure/README.md](../azure/README.md)).
Ce document ne rapporte que ce qui a été **exécuté et observé** ; les décisions en attente
sont marquées comme telles. Les coûts réels seront ajoutés au fil des étapes.

**Statut au 2026-10-10 : infrastructure minimale en place, ingestion Open-Meteo en cours de
validation (étape 2b). Databricks : décision en attente.**

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
| Key Vault `kv-climatagri-<suffix>` | créé | autorisation RBAC, suppression logique ; secrets `sql-user` et `sql-password` (mot de passe généré, jamais affiché ni écrit dans un fichier) |
| Azure SQL (serveur + base `sqldb-climatagri`) | créé | General Purpose serverless, offre « free database » avec `AutoPause` ; authentification SQL conservée, admin Entra ajouté ; règles de pare-feu `AllowAzureServices` et `AllowMyIP` (temporaire) |
| Data Factory `adf-climatagri-<suffix>` | créé | identité managée système, rôle *Storage Blob Data Contributor* sur le Storage |
| Ressources ADF (ingestion) | déployées | 3 services liés, 5 datasets, 2 pipelines, 2 triggers **arrêtés** |
| Ressources `dbg_*` | temporaires | sondes de diagnostic, à supprimer en fin de démonstration |
| Databricks, `transform_databricks`, `ls_databricks`, `ls_azuresql` | non déployés | décision en attente |

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

## Databricks : décision en attente

Le job cluster prévu (`Standard_DS3_v2`, single-node, driver seul, 0 worker) utilise 4 vCPU,
soit exactement le quota. Un cluster classique driver + worker (8 vCPU) est impossible.

Ce qui est établi, et ce qui ne l'est pas :

- Des réponses de la communauté Microsoft Q&A indiquent que les abonnements d'essai gratuits
  sont limités à 4 cœurs, que seul un cluster **Single Node** y fonctionne, et qu'il faut passer
  en Pay-as-you-go pour un cluster multi-nœuds. Ce ne sont pas des documents officiels et
  certains rapports mentionnent des échecs à la création du cluster : **non vérifié sur notre
  abonnement**.
- Le workspace ne coûte rien tant qu'aucun cluster ne tourne (hors petit stockage géré par
  Databricks). Seul le démarrage d'un cluster consomme des DBU et des VM.
- Plan : créer le workspace en dernier, tenter un cluster single-node avec arrêt automatique à
  10 minutes. S'il démarre, il exécute les notebooks. Sinon, suppression du workspace et repli
  sur PySpark local (WSL2), sans changer le code de `azure/databricks/`.
- Passer en Pay-as-you-go est **exclu** : il supprime la limite de dépense.

Décision finale : à consigner ici après le test.

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
   Key Vault créé par le script. *(À trancher avec Databricks.)*
7. Databricks Premium et Unity Catalog configurés à la main : à simplifier. *(En attente.)*
8. Le déploiement ADF envoie tous les dossiers : sans Databricks, `ls_databricks`,
   `transform_databricks` et `tr_daily_transform` doivent être exclus. *(Traité par
   `deploy_adf_ingest.sh`.)*
9. L'ingestion REST échoue (voir ci-dessus) : remplacée par le connecteur HTTP en mode binaire.

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
(`az keyvault purge`). Avant cela : supprimer la règle de pare-feu `AllowMyIP` et les ressources
`dbg_*` si le groupe de ressources est conservé.
