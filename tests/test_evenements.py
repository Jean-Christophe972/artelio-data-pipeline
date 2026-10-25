"""Contrats de données : un événement valide passe, chaque défaut est rejeté avec une raison claire."""

import datetime as dt
import json

import pytest
from conftest import encoder

from artelio_pipeline.evenements import DemandeCreee, EvenementInvalide, valider


def test_demande_valide(demande):
    evt = valider("demandes.creees", encoder(demande()))
    assert isinstance(evt, DemandeCreee)
    assert evt.canal == "app"


def test_description_normalisee(demande):
    evt = valider("demandes.creees", encoder(demande(description="  Fuite   d'eau\n sous l'évier, ça coule.  ")))
    assert evt.description == "Fuite d'eau sous l'évier, ça coule."


@pytest.mark.parametrize(
    ("changement", "raison"),
    [
        ({"canal": "pigeon voyageur"}, "schema_invalide"),
        ({"description": "trop court"}, "schema_invalide"),
        ({"client_id": "pas-un-uuid"}, "schema_invalide"),
        ({"champ_inconnu": 1}, "schema_invalide"),
        ({"emis_le": "2026-10-25T10:00:00"}, "schema_invalide"),  # sans fuseau horaire
    ],
)
def test_demande_invalide(demande, changement, raison):
    with pytest.raises(EvenementInvalide) as exc:
        valider("demandes.creees", encoder(demande(**changement)))
    assert exc.value.raison == raison


def test_champ_manquant(demande):
    evt = demande()
    del evt["logement_id"]
    with pytest.raises(EvenementInvalide, match="logement_id"):
        valider("demandes.creees", encoder(evt))


def test_date_future_rejetee(demande):
    futur = (dt.datetime.now(dt.UTC) + dt.timedelta(days=1)).isoformat()
    with pytest.raises(EvenementInvalide, match="futur"):
        valider("demandes.creees", encoder(demande(emis_le=futur)))


def test_json_casse():
    with pytest.raises(EvenementInvalide) as exc:
        valider("demandes.creees", b'{"event_id": "abc"')
    assert exc.value.raison == "json_invalide"


def test_message_vide():
    with pytest.raises(EvenementInvalide) as exc:
        valider("demandes.creees", None)
    assert exc.value.raison == "message_vide"


def test_position_hors_ile_de_france():
    evt = {
        "event_id": "6f1c6a3e-6b4f-4f3e-9a8a-1c2d3e4f5a6b",
        "type": "artisan.disponibilite",
        "emis_le": dt.datetime.now(dt.UTC).isoformat(),
        "artisan_id": "1f1c6a3e-6b4f-4f3e-9a8a-1c2d3e4f5a6b",
        "latitude": 43.3,  # Marseille
        "longitude": 5.4,
        "disponible": True,
    }
    with pytest.raises(EvenementInvalide, match="latitude"):
        valider("artisans.disponibilite", json.dumps(evt).encode())


def test_topic_inconnu(demande):
    with pytest.raises(EvenementInvalide) as exc:
        valider("topic.inexistant", encoder(demande()))
    assert exc.value.raison == "topic_inconnu"
