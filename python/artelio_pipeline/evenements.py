"""Contrats de données : le schéma de chaque événement temps réel.

Chaque événement Kafka est validé contre son modèle Pydantic AVANT d'entrer en base.
Un événement invalide n'est jamais perdu : il part dans la file d'erreurs (DLQ) avec
la raison du rejet, pour analyse et correction.
"""

from __future__ import annotations

import datetime as dt
import json
import uuid
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

# Emprise géographique de l'Île-de-France (contrôle de vraisemblance des positions GPS)
LAT_MIN, LAT_MAX = 48.10, 49.25
LON_MIN, LON_MAX = 1.40, 3.60

Latitude = Annotated[float, Field(ge=LAT_MIN, le=LAT_MAX)]
Longitude = Annotated[float, Field(ge=LON_MIN, le=LON_MAX)]


class Evenement(BaseModel):
    """Enveloppe commune : identifiant unique (idempotence), type, date d'émission, version du schéma."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: uuid.UUID
    emis_le: dt.datetime
    version: Literal[1] = 1

    @field_validator("emis_le")
    @classmethod
    def _date_vraisemblable(cls, value: dt.datetime) -> dt.datetime:
        if value.tzinfo is None:
            raise ValueError("la date doit préciser son fuseau horaire")
        maintenant = dt.datetime.now(dt.UTC)
        if value > maintenant + dt.timedelta(minutes=5):
            raise ValueError("date d'émission dans le futur")
        if value < maintenant - dt.timedelta(days=7):
            raise ValueError("événement de plus de 7 jours (au-delà de la rétention Kafka)")
        return value


class DemandeCreee(Evenement):
    type: Literal["demande.creee"]
    demande_id: uuid.UUID
    client_id: uuid.UUID
    logement_id: uuid.UUID
    description: Annotated[str, Field(min_length=20, max_length=2000)]
    canal: Literal["app", "web", "vocal", "telephone"]

    @field_validator("description")
    @classmethod
    def _normaliser(cls, value: str) -> str:
        return " ".join(value.split())  # espaces multiples, retours à la ligne


class MessageEnvoye(Evenement):
    type: Literal["message.envoye"]
    message_id: uuid.UUID
    demande_id: uuid.UUID
    auteur_type: Literal["client", "artisan", "support", "systeme"]
    auteur_id: uuid.UUID | None = None
    contenu: Annotated[str, Field(min_length=1, max_length=5000)]
    metadonnees: dict[str, Any] = Field(default_factory=dict)


class DisponibiliteArtisan(Evenement):
    type: Literal["artisan.disponibilite"]
    artisan_id: uuid.UUID
    latitude: Latitude
    longitude: Longitude
    disponible: bool


class StatutIntervention(Evenement):
    type: Literal["intervention.statut"]
    intervention_id: uuid.UUID
    statut: Literal["en_cours", "terminee", "annulee"]


MODELES: dict[str, type[Evenement]] = {
    "demandes.creees": DemandeCreee,
    "messages.envoyes": MessageEnvoye,
    "artisans.disponibilite": DisponibiliteArtisan,
    "interventions.statut": StatutIntervention,
}


class EvenementInvalide(Exception):
    """Rejet d'un événement, avec une raison courte (utilisée comme étiquette de métrique)."""

    def __init__(self, raison: str, detail: str):
        super().__init__(f"{raison}: {detail}")
        self.raison = raison
        self.detail = detail


def valider(topic: str, brut: bytes | None) -> Evenement:
    """Désérialise et valide un message Kafka. Lève EvenementInvalide en cas de problème."""
    modele = MODELES.get(topic)
    if modele is None:
        raise EvenementInvalide("topic_inconnu", topic)
    if not brut:
        raise EvenementInvalide("message_vide", "aucun contenu")
    try:
        donnees = json.loads(brut)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise EvenementInvalide("json_invalide", str(exc)[:200]) from exc
    try:
        return modele.model_validate(donnees)
    except ValidationError as exc:
        premiere = exc.errors()[0]
        champ = ".".join(str(p) for p in premiere["loc"]) or "racine"
        raise EvenementInvalide("schema_invalide", f"{champ} : {premiere['msg']}") from exc
