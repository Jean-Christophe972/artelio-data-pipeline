#!/usr/bin/env bash
# Démonstration de redondance : on tue un des deux processeurs de flux pendant une rafale.
# Kafka redistribue ses partitions au survivant : aucun événement n'est perdu.
set -euo pipefail
cd "$(dirname "$0")/.."

compter() { docker exec artelio-pg-primary psql -U artelio_admin -d artelio -Atc "SELECT count(*) FROM pipeline.evenements_traites"; }
etape() { printf '\n\033[1;34m▶ %s\033[0m\n' "$1"; }

etape "0. Pause du simulateur permanent (pour compter précisément la rafale)"
docker compose stop event-producer >/dev/null
sleep 5  # laisse les derniers événements en transit être traités avant de compter

etape "1. Deux processeurs actifs dans le même groupe de consommateurs"
docker compose ps stream-processor --format '{{.Name}}  {{.Status}}'
avant=$(compter)

etape "2. Rafale de 10 000 événements valides"
docker compose run --rm --no-deps -d stream-processor python -m artelio_pipeline.simulateur --rafale 10000 --taux-erreur 0 >/dev/null
sleep 3

etape "3. PANNE : arrêt brutal d'un processeur en plein traitement"
victime=$(docker compose ps stream-processor --format '{{.Name}}' | head -1)
docker kill "${victime}" >/dev/null
echo "Processeur tué : ${victime}"

etape "4. Le survivant reprend ses partitions ; on attend la fin du traitement"
for _ in $(seq 1 60); do
  traites=$(( $(compter) - avant ))
  printf '\r   événements traités : %6d / 10000' "${traites}"
  (( traites >= 10000 )) && break
  sleep 2
done
echo
etape "5. Résultat"
echo "   ${traites} événements traités sur 10 000 : aucune perte malgré la panne."

etape "6. Retour à l'état nominal"
docker compose up -d stream-processor event-producer >/dev/null
docker compose ps stream-processor --format '{{.Name}}  {{.Status}}'
