#!/usr/bin/env bash
# Wrapper appele par les DAGs Airflow (BashOperator) : traduit l'exit code 1
# de ingestion.run (succes partiel/degrade - chunks en echec ou agriculture
# en mock-fallback-network-error) en succes Airflow (exit 0), pour eviter des
# retries inutiles sur un resultat qui n'est pas vraiment un echec.
# Exit 0 et 2 (erreur fatale) sont propages tels quels.
set -uo pipefail

python3 -m ingestion.run "$@"
exit_code=$?

if [ "$exit_code" -eq 1 ]; then
    echo "WARNING: ingestion partielle (chunks en echec ou agriculture degradee) - traitee comme succes Airflow"
    exit 0
fi

exit "$exit_code"
