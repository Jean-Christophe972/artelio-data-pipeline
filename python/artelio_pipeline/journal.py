"""Traçabilité des exécutions (lineage) : chaque tâche note ce qu'elle a lu et écrit.

with journaliser(cfg, "artelio_dwh_incremental", run_id, "charger_faits") as j:
    ...
    j.lignes_ecrites = 1200
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

from artelio_pipeline.config import Config
from artelio_pipeline.db import connecter
from artelio_pipeline.metriques import maintenant, pousser

log = logging.getLogger(__name__)


@dataclass
class Execution:
    lignes_lues: int | None = None
    lignes_ecrites: int | None = None
    details: dict = field(default_factory=dict)


@contextmanager
def journaliser(cfg: Config, dag_id: str, run_id: str, tache: str) -> Iterator[Execution]:
    execution = Execution()
    with connecter(cfg, autocommit=True) as conn:
        id_ = conn.execute(
            "INSERT INTO pipeline.journal_executions (dag_id, run_id, tache, debut, statut) "
            "VALUES (%s, %s, %s, now(), 'en_cours') RETURNING id",
            (dag_id, run_id, tache),
        ).fetchone()[0]
    statut = "echec"
    try:
        yield execution
        statut = "succes"
    finally:
        with connecter(cfg, autocommit=True) as conn:
            conn.execute(
                """UPDATE pipeline.journal_executions
                   SET fin = now(), statut = %s, lignes_lues = %s, lignes_ecrites = %s, details = %s WHERE id = %s""",
                (statut, execution.lignes_lues, execution.lignes_ecrites, json.dumps(execution.details, default=str), id_),
            )


def signaler_echec(context: dict) -> None:
    """Callback Airflow : appelé quand une tâche échoue après toutes ses tentatives.

    Publie l'horodatage de l'échec ; Prometheus déclenche l'alerte TacheAirflowEnEchec.
    """
    dag_id = context["dag"].dag_id
    tache = context["task_instance"].task_id
    log.error("Échec définitif de %s.%s : %s", dag_id, tache, context.get("exception"))
    pousser(
        Config().pushgateway,
        f"airflow_{dag_id}",
        {("artelio_dag_dernier_echec_timestamp_seconds", (("dag", dag_id), ("tache", tache))): maintenant()},
    )
