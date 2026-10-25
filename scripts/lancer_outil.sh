#!/usr/bin/env bash
# Lance un module du pipeline dans un conteneur ponctuel, sur le réseau de la plateforme.
#   ./scripts/lancer_outil.sh benchmark --evenements 20000
#   ./scripts/lancer_outil.sh simulateur --rafale 5000 --taux-erreur 0.2
set -euo pipefail
cd "$(dirname "$0")/.."
MODULE="${1:?Usage : lancer_outil.sh <module> [arguments]}"
shift
docker compose run --rm --no-deps stream-processor python -m "artelio_pipeline.${MODULE}" "$@"
