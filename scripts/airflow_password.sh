#!/usr/bin/env bash
# Affiche le mot de passe de l'interface Airflow (utilisateur : admin).
set -euo pipefail
grep -E '^AIRFLOW_ADMIN_PASSWORD=' "$(cd "$(dirname "$0")/.." && pwd)/.env" | cut -d= -f2-
