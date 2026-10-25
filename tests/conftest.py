"""Fixtures partagées : fabrique d'événements valides."""

import datetime as dt
import json
import uuid

import pytest


@pytest.fixture
def demande():
    def fabriquer(**changements):
        evt = {
            "event_id": str(uuid.uuid4()),
            "type": "demande.creee",
            "emis_le": dt.datetime.now(dt.UTC).isoformat(),
            "version": 1,
            "demande_id": str(uuid.uuid4()),
            "client_id": str(uuid.uuid4()),
            "logement_id": str(uuid.uuid4()),
            "description": "Fuite d'eau importante sous l'évier de la cuisine.",
            "canal": "app",
        }
        evt.update(changements)
        return evt

    return fabriquer


def encoder(evt: dict) -> bytes:
    return json.dumps(evt).encode()
