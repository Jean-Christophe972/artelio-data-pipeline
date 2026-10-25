"""Préparation d'un lot : tri entre valides et rejets, doublons éliminés dans le lot."""

from conftest import encoder

from artelio_pipeline.stream_processor import preparer_lot


def test_lot_mixte(demande):
    valide = demande()
    lot = preparer_lot(
        [
            ("demandes.creees", encoder(valide), 0, 1),
            ("demandes.creees", encoder(valide), 0, 2),  # même event_id : doublon
            ("demandes.creees", b"{casse", 0, 3),
            ("demandes.creees", encoder(demande(canal="fax")), 1, 4),
        ]
    )
    assert len(lot.valides["demandes.creees"]) == 1
    assert {r.raison for r in lot.rejets} == {"json_invalide", "schema_invalide"}
    # Le rejet garde sa position dans Kafka pour pouvoir le retrouver
    assert {(r.partition, r.offset) for r in lot.rejets} == {(0, 3), (1, 4)}


def test_lot_vide():
    lot = preparer_lot([])
    assert lot.valides == {} and lot.rejets == []
