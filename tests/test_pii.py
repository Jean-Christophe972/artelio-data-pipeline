"""Masquage des données personnelles dans les textes libres."""

import pytest

from artelio_pipeline.pii import contient_donnees_personnelles, masquer


@pytest.mark.parametrize(
    "texte",
    [
        "Rappelez-moi au 06 12 34 56 78 svp",
        "tel 0612345678",
        "mon numéro : 06.12.34.56.78",
        "joignable au +33 6 12 34 56 78",
        "ou au 0033612345678",
        "fixe 01-42-34-56-78",
    ],
)
def test_telephones_masques(texte):
    resultat, n = masquer(texte)
    assert n == 1
    assert "[TELEPHONE]" in resultat
    assert not contient_donnees_personnelles(resultat)


def test_email_masque():
    resultat, n = masquer("écrivez à jean.dupont+travaux@example.org merci")
    assert resultat == "écrivez à [EMAIL] merci"
    assert n == 1


def test_iban_masque():
    resultat, _ = masquer("virement sur FR76 3000 6000 0112 3456 7890 189")
    assert "[IBAN]" in resultat


@pytest.mark.parametrize(
    "texte",
    [
        "Devis de 150 € pour 2 heures de travail",
        "Intervention prévue le 12/11/2026 à 14h30",
        "Surface d'environ 30 m2, code porte 4521",
        "Chaudière de 2015, code erreur E133",
    ],
)
def test_pas_de_faux_positif(texte):
    resultat, n = masquer(texte)
    assert n == 0
    assert resultat == texte
