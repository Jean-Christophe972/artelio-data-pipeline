"""Intégrité des DAG Airflow : chargement sans erreur, planification et reprise configurées.

Ce test s'exécute là où Airflow est installé (conteneur Airflow, job CI dédié).
"""

from pathlib import Path

import pytest

airflow_models = pytest.importorskip("airflow.models")

DAGS = Path(__file__).resolve().parents[1] / "dags"
ATTENDUS = {"artelio_dwh_incremental", "artelio_qualite_donnees", "artelio_rgpd_purge", "artelio_dataset_ia"}


@pytest.fixture(scope="module")
def dagbag():
    return airflow_models.DagBag(dag_folder=str(DAGS), include_examples=False)


def test_aucune_erreur_import(dagbag):
    assert dagbag.import_errors == {}


def test_dags_attendus(dagbag):
    assert set(dagbag.dag_ids) == ATTENDUS


@pytest.mark.parametrize("dag_id", sorted(ATTENDUS))
def test_reprise_et_alerte(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    assert dag.schedule_interval is not None
    assert dag.max_active_runs == 1
    for tache in dag.tasks:
        assert tache.retries >= 3
        assert tache.on_failure_callback is not None
