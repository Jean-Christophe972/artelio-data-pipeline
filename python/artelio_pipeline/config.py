"""Configuration du pipeline, lue depuis les variables d'environnement.

Les secrets viennent du fichier .env généré par scripts/configure_env.sh à partir
des sorties Terraform de la plateforme (bloc 2) : rien n'est écrit dans le code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Variable d'environnement manquante : {name}")
    return value


@dataclass(frozen=True)
class Config:
    # PostgreSQL (compte pipeline_etl : lecture du métier, écriture de l'entrepôt)
    pg_host: str = field(default_factory=lambda: _env("PG_HOST", "pg-primary"))
    pg_port: int = field(default_factory=lambda: int(_env("PG_PORT", "5432")))
    pg_database: str = field(default_factory=lambda: _env("PG_DATABASE", "artelio"))
    pg_user: str = field(default_factory=lambda: _env("PG_USER", "pipeline_etl"))
    pg_password: str = field(default_factory=lambda: _env("PG_PASSWORD"))

    # Kafka
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "kafka:9092"))
    kafka_group: str = field(default_factory=lambda: _env("KAFKA_GROUP", "artelio-stream-processor"))
    topic_dlq: str = "dlq.erreurs"

    # Stockage objet (identité S3 "pipeline" : photos et datasets uniquement)
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://object-storage:8333"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY"))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY"))

    # Supervision
    pushgateway: str = field(default_factory=lambda: _env("PUSHGATEWAY", "pushgateway:9091"))
    metrics_port: int = field(default_factory=lambda: int(_env("METRICS_PORT", "8000")))

    # Traitement par lots du flux : compromis débit / latence
    taille_lot: int = field(default_factory=lambda: int(_env("TAILLE_LOT", "500")))
    attente_lot_s: float = field(default_factory=lambda: float(_env("ATTENTE_LOT_S", "1.0")))


TOPICS_ENTRANTS = ["demandes.creees", "messages.envoyes", "artisans.disponibilite", "interventions.statut"]
