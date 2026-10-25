"""Alimentation incrémentale de l'entrepôt en étoile, toutes les 15 minutes.

app (OLTP) ──► dimensions (client, métier, artisan SCD 2) ──► faits (demande, intervention)
Tout est fait dans une seule transaction et de façon idempotente (upserts).
"""

from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from commun import ARGUMENTS_PAR_DEFAUT, DEBUT


@dag(
    dag_id="artelio_dwh_incremental",
    schedule="*/15 * * * *",
    start_date=DEBUT,
    catchup=False,
    max_active_runs=1,  # deux chargements ne se chevauchent jamais
    default_args=ARGUMENTS_PAR_DEFAUT,
    tags=["artelio", "entrepot"],
    doc_md=__doc__,
)
def artelio_dwh_incremental():
    @task
    def charger_entrepot() -> dict:
        from artelio_pipeline import dwh
        from artelio_pipeline.config import Config
        from artelio_pipeline.journal import journaliser

        cfg = Config()
        with journaliser(cfg, "artelio_dwh_incremental", get_current_context()["run_id"], "charger_entrepot") as j:
            resultat = dwh.charger(cfg)
            j.lignes_ecrites = sum(resultat["lignes"].values())
            j.details = resultat
        return resultat

    charger_entrepot()


artelio_dwh_incremental()
