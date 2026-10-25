"""Purge quotidienne des données expirées (durées de conservation, plan de gouvernance §5.2).

Positions GPS > 24 h, données de santé expirées, photos > 12 mois après l'intervention
(supprimées aussi du stockage objet), comptes inactifs depuis 3 ans.
"""

from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from commun import ARGUMENTS_PAR_DEFAUT, DEBUT


@dag(
    dag_id="artelio_rgpd_purge",
    schedule="0 3 * * *",  # chaque nuit à 3 h, heure creuse
    start_date=DEBUT,
    catchup=False,
    max_active_runs=1,
    default_args=ARGUMENTS_PAR_DEFAUT,
    tags=["artelio", "rgpd"],
    doc_md=__doc__,
)
def artelio_rgpd_purge():
    @task
    def purger() -> dict:
        from artelio_pipeline import rgpd
        from artelio_pipeline.config import Config
        from artelio_pipeline.journal import journaliser

        cfg = Config()
        with journaliser(cfg, "artelio_rgpd_purge", get_current_context()["run_id"], "purger") as j:
            resultat = rgpd.purger(cfg)
            j.details = resultat
        return resultat

    purger()


artelio_rgpd_purge()
