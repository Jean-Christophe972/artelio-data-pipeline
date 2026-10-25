"""Barrière qualité du jeu d'entraînement : rien n'est publié s'il pose problème."""

import pytest

from artelio_pipeline.dataset import METIERS, DatasetRefuse, valider_dataset


def jeu(n_par_metier=200, **remplacement):
    lignes = []
    for i, metier in enumerate(sorted(METIERS) * n_par_metier):
        ligne = {"demande_pseudo": f"p{i}", "texte": "fuite sous l'évier", "metier": metier, "urgence": "normale"}
        lignes.append(ligne | remplacement)
    return lignes


def test_jeu_conforme():
    valider_dataset(jeu())


def test_volume_insuffisant():
    with pytest.raises(DatasetRefuse, match="volume insuffisant"):
        valider_dataset(jeu(n_par_metier=10))


def test_classe_absente():
    lignes = [ligne for ligne in jeu() if ligne["metier"] != "serrurerie"]
    with pytest.raises(DatasetRefuse, match="serrurerie"):
        valider_dataset(lignes)


def test_donnee_personnelle_residuelle():
    lignes = jeu()
    lignes[0]["texte"] = "rappelez au 06 12 34 56 78"
    with pytest.raises(DatasetRefuse, match="données personnelles"):
        valider_dataset(lignes)


def test_doublons():
    lignes = jeu()
    lignes[1]["demande_pseudo"] = lignes[0]["demande_pseudo"]
    with pytest.raises(DatasetRefuse, match="doublons"):
        valider_dataset(lignes)


def test_urgence_inconnue():
    with pytest.raises(DatasetRefuse, match="urgences inconnues"):
        valider_dataset(jeu(urgence="apocalyptique"))
