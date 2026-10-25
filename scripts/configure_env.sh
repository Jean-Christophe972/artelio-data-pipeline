#!/usr/bin/env bash
# Génère le fichier .env (non versionné) du pipeline :
#   - secrets de la plateforme (bloc 2) lus dans les sorties Terraform ;
#   - secrets propres à Airflow, générés une seule fois puis conservés.
#
#   PLATFORM_DIR=<chemin du dépôt artelio-data-platform> ./scripts/configure_env.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PLATFORM_DIR="${PLATFORM_DIR:-${ROOT}/../../Bloc2_Architecture/artelio-data-platform}"
ENV_FILE="${ROOT}/.env"

command -v terraform >/dev/null || { echo "Terraform est requis (plateforme du bloc 2)." >&2; exit 1; }
[[ -d "${PLATFORM_DIR}/terraform" ]] || { echo "Plateforme introuvable : ${PLATFORM_DIR} (variable PLATFORM_DIR)" >&2; exit 1; }

secrets=$(terraform -chdir="${PLATFORM_DIR}/terraform" output -json secrets)
lire() { python3 -c "import sys, json; print(json.load(sys.stdin)['$1'])" <<<"${secrets}"; }
aleatoire() { python3 -c "import secrets; print(secrets.token_urlsafe(24))"; }

# Les secrets Airflow déjà générés sont conservés (sinon Airflow ne pourrait plus déchiffrer ses données)
existant() { [[ -f "${ENV_FILE}" ]] && grep -E "^$1=" "${ENV_FILE}" | cut -d= -f2- || true; }
AIRFLOW_DB_PASSWORD=$(existant AIRFLOW_DB_PASSWORD); AIRFLOW_DB_PASSWORD=${AIRFLOW_DB_PASSWORD:-$(aleatoire)}
AIRFLOW_SECRET_KEY=$(existant AIRFLOW_SECRET_KEY); AIRFLOW_SECRET_KEY=${AIRFLOW_SECRET_KEY:-$(aleatoire)}
AIRFLOW_ADMIN_PASSWORD=$(existant AIRFLOW_ADMIN_PASSWORD); AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD:-$(aleatoire)}
AIRFLOW_FERNET_KEY=$(existant AIRFLOW_FERNET_KEY)
AIRFLOW_FERNET_KEY=${AIRFLOW_FERNET_KEY:-$(python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")}

umask 077
cat > "${ENV_FILE}" <<ENV
# Généré par scripts/configure_env.sh — NE PAS VERSIONNER
PG_ETL_PASSWORD=$(lire postgres_pipeline)
S3_PIPELINE_ACCESS=$(lire s3_pipeline_access)
S3_PIPELINE_SECRET=$(lire s3_pipeline_secret)
AIRFLOW_DB_PASSWORD=${AIRFLOW_DB_PASSWORD}
AIRFLOW_SECRET_KEY=${AIRFLOW_SECRET_KEY}
AIRFLOW_FERNET_KEY=${AIRFLOW_FERNET_KEY}
AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD}
DEBIT_EVENEMENTS=20
ENV
echo "Fichier .env écrit (droits 600). Airflow : utilisateur admin, mot de passe : ./scripts/airflow_password.sh"
