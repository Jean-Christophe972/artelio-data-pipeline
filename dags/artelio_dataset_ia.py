"""Publication quotidienne du jeu d'entraînement du modèle IA (consommé par le bloc 4).

extraire (hors opposants et comptes effacés) → masquer les données personnelles →
barrière qualité → publier une version dans S3 (datasets/demandes/<version>/ + manifeste).
Si la barrière qualité refuse le jeu, rien n'est publié et le modèle garde la version précédente.
"""

from __future__ import annotations

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from commun import ARGUMENTS_PAR_DEFAUT, DEBUT


@dag(
    dag_id="artelio_dataset_ia",
    schedule="0 4 * * *",  # après la purge RGPD de 3 h : le jeu ne contient plus de données expirées
    start_date=DEBUT,
    catchup=False,
    max_active_runs=1,
    default_args=ARGUMENTS_PAR_DEFAUT,
    tags=["artelio", "ia"],
    doc_md=__doc__,
)
def artelio_dataset_ia():
    @task
    def publier_jeu_entrainement() -> dict:
        from artelio_pipeline import dataset
        from artelio_pipeline.config import Config
        from artelio_pipeline.journal import journaliser

        cfg = Config()
        with journaliser(cfg, "artelio_dataset_ia", get_current_context()["run_id"], "publier_jeu_entrainement") as j:
            lignes, rapport = dataset.extraire(cfg)
            j.lignes_lues = len(lignes)
            manifest = dataset.publier(cfg, lignes, rapport)
            j.lignes_ecrites = manifest["lignes"]
            j.details = {"version": manifest["version"], "sha256": manifest["sha256"]}
        return {"version": manifest["version"], "lignes": manifest["lignes"]}

    publier_jeu_entrainement()


artelio_dataset_ia()
