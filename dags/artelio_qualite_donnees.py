"""Contrôles qualité des données toutes les 30 minutes (plan de gouvernance, §6).

9 règles (exhaustivité, exactitude, unicité, validité, fraîcheur, cohérence) + détection
d'opérations massives suspectes. Résultats historisés et publiés vers Prometheus.
Une règle non conforme déclenche une alerte ; elle ne fait pas échouer le DAG
(le DAG n'échoue que sur une erreur technique).
"""

from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from commun import ARGUMENTS_PAR_DEFAUT, DEBUT


@dag(
    dag_id="artelio_qualite_donnees",
    schedule="*/30 * * * *",
    start_date=DEBUT,
    catchup=False,
    max_active_runs=1,
    default_args=ARGUMENTS_PAR_DEFAUT,
    tags=["artelio", "qualite"],
    doc_md=__doc__,
)
def artelio_qualite_donnees():
    @task
    def controler() -> dict:
        from artelio_pipeline import qualite
        from artelio_pipeline.config import Config
        from artelio_pipeline.journal import journaliser

        cfg = Config()
        with journaliser(cfg, "artelio_qualite_donnees", get_current_context()["run_id"], "controler") as j:
            resultats = qualite.executer(cfg)
            synthese = {r.regle.nom: {"taux": round(r.taux, 4), "conforme": r.conforme} for r in resultats}
            j.lignes_lues = sum(r.total for r in resultats)
            j.details = synthese
        return synthese

    controler()


artelio_qualite_donnees()
