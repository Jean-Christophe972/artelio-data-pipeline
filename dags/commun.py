"""Paramètres communs aux DAG Artelio : reprise automatique, alerte en cas d'échec définitif."""

from __future__ import annotations

from datetime import timedelta

import pendulum

from artelio_pipeline.journal import signaler_echec

DEBUT = pendulum.datetime(2026, 10, 1, tz="Europe/Paris")

ARGUMENTS_PAR_DEFAUT = {
    "owner": "equipe-data",
    # Reprise automatique : 3 nouvelles tentatives, avec une attente qui double à chaque fois
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
    # Après la dernière tentative : métrique d'échec → alerte Prometheus TacheAirflowEnEchec
    "on_failure_callback": signaler_echec,
}
