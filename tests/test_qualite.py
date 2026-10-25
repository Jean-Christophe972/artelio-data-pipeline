"""Évaluation des règles de qualité (taux de conformité et seuils)."""

from artelio_pipeline.qualite import REGLES, Regle, Resultat

REGLE = Regle("test", "exhaustivité", 0.99, "SELECT 1, 0", "règle de test")


def test_taux_et_conformite():
    assert Resultat(REGLE, total=1000, en_erreur=5).conforme  # 99,5 % ≥ 99 %
    assert not Resultat(REGLE, total=1000, en_erreur=20).conforme  # 98 % < 99 %


def test_table_vide_conforme():
    assert Resultat(REGLE, total=0, en_erreur=0).taux == 1.0


def test_regles_bien_definies():
    noms = [r.nom for r in REGLES]
    assert len(noms) == len(set(noms)), "noms de règles en double"
    dimensions = {r.dimension for r in REGLES}
    # Les 6 dimensions du plan de gouvernance (§6) sont couvertes
    assert dimensions == {"exhaustivité", "exactitude", "unicité", "validité", "fraîcheur", "cohérence"}
    assert all(0 < r.seuil <= 1 for r in REGLES)
