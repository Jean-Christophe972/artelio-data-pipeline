"""Accès à PostgreSQL (TLS obligatoire) et application des migrations du schéma "pipeline"."""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

import psycopg

from artelio_pipeline.config import Config

log = logging.getLogger(__name__)
# Dossier /sql du dépôt (monté dans les conteneurs ; surchargeable par ARTELIO_SQL_DIR)
SQL_DIR = Path(os.environ.get("ARTELIO_SQL_DIR", Path(__file__).resolve().parents[2] / "sql"))


def connecter(cfg: Config, autocommit: bool = False, tentatives: int = 30) -> psycopg.Connection:
    """Ouvre une connexion, en réessayant tant que la base démarre."""
    for essai in range(1, tentatives + 1):
        try:
            return psycopg.connect(
                host=cfg.pg_host,
                port=cfg.pg_port,
                dbname=cfg.pg_database,
                user=cfg.pg_user,
                password=cfg.pg_password,
                sslmode="require",
                autocommit=autocommit,
                connect_timeout=5,
                application_name="artelio-pipeline",
            )
        except psycopg.OperationalError as exc:
            if essai == tentatives:
                raise
            log.warning("PostgreSQL indisponible (%s/%s) : %s", essai, tentatives, exc)
            time.sleep(min(2 * essai, 10))
    raise AssertionError("inatteignable")


def lire_sql(chemin: str) -> str:
    """Lit un fichier du dossier /sql (ex. "dwh/03_fait_demande.sql")."""
    return (SQL_DIR / chemin).read_text(encoding="utf-8")


def appliquer_migrations(cfg: Config) -> list[str]:
    """Applique, dans l'ordre et une seule fois, les fichiers de sql/migrations/."""
    appliquees = []
    with connecter(cfg) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS pipeline.schema_migrations ("
            " version text PRIMARY KEY, appliquee_le timestamptz NOT NULL DEFAULT now())"
        )
        deja = {row[0] for row in conn.execute("SELECT version FROM pipeline.schema_migrations")}
        for fichier in sorted((SQL_DIR / "migrations").glob("*.sql")):
            if fichier.stem in deja:
                continue
            log.info("Migration %s", fichier.name)
            conn.execute(fichier.read_text(encoding="utf-8"))
            conn.execute("INSERT INTO pipeline.schema_migrations (version) VALUES (%s)", (fichier.stem,))
            appliquees.append(fichier.stem)
        conn.commit()
    return appliquees
