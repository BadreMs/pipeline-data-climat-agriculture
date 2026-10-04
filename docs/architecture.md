# Architecture

> Document en cours de rédaction : la version complète est prévue en Phase 6.
> Cette page ne contient pour l'instant que les limites connues et les notes à intégrer.

## Known limitations

### Ingestion : les chunks perdus sont masqués lors d'un backfill long

Constaté lors du premier backfill (2015 → 2026, 12 régions) : deux chunks de 2 ans ont été
perdus sans erreur visible (MA-11 : 2017-2018, MA-08 : 2021-2022, soit 730 jours chacun).
Réparés manuellement en relançant `ingestion.run` sur ces régions et ces périodes.

- `airflow/scripts/run_ingestion.sh` traduit l'exit code 1 de `ingestion.run` (chunks en
  échec) en exit 0, pour éviter des retries inutiles. Effet de bord : un backfill long peut
  perdre des chunks sans qu'aucun signal n'apparaisse dans Airflow.
- Amélioration prévue : écrire un récapitulatif explicite des `failed_chunks` dans un fichier
  (par ex. `/tmp/last_ingest_failures.json`) et l'exposer via XCom Airflow ou via un contrôle
  de complétude dbt.

### dbt : pas de contrôle de complétude des données météo

Aucun test dbt ne détecte les jours manquants : les trous ci-dessus n'ont fait échouer aucun
des 51 tests. Test à ajouter si le temps le permet : `days_observed >= 350` dans
`int_weather_agriculture_join` pour les années complètes.

## Notes à intégrer en Phase 6

- **dbt 1.8** : `dbt-core>=1.9` est incompatible avec les contraintes Airflow 2.9.3
  (protobuf, python-dotenv, sqlparse). Conséquence visible dans `uv.lock` : mypy borné à 1.x.
  Alternative : venv dédié à dbt dans l'image Airflow.
- **Windows / Git Bash** : `docker compose exec ... /opt/...` convertit les chemins
  (`C:\Program Files\Git\opt\...`). Préfixer la commande par `MSYS_NO_PATHCONV=1` ou utiliser
  `//opt/...`.
- **Image Airflow** : `git` n'est pas installé (warning cosmétique de `dbt debug`).
- **Données agricoles** : fixture synthétique tant qu'aucune URL data.gov.ma n'est configurée
  (`agriculture_is_mock` dans les marts).
