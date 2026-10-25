"""Simulateur d'événements : remplace l'application mobile pour alimenter Kafka.

    python -m artelio_pipeline.simulateur --debit 20 --taux-erreur 0.03
    python -m artelio_pipeline.simulateur --rafale 20000       # envoie 20 000 événements puis s'arrête

Les événements référencent de vrais clients, logements, artisans et interventions lus en base.
Une petite part est volontairement défectueuse (champ manquant, valeur interdite, JSON cassé,
référence inconnue, doublon…) pour démontrer les contrôles qualité et la file d'erreurs.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import random
import signal
import sys
import time
import uuid

from confluent_kafka import Producer

from artelio_pipeline.config import Config
from artelio_pipeline.db import connecter
from artelio_pipeline.textes import generer_description, poids_metiers

log = logging.getLogger("simulateur")

# Répartition des types d'événements (les messages sont les plus fréquents)
POIDS_TYPES = {"demandes.creees": 25, "messages.envoyes": 45, "artisans.disponibilite": 25, "interventions.statut": 5}
DEFAUTS = ["champ_manquant", "valeur_interdite", "json_casse", "reference_inconnue", "gps_hors_zone", "doublon", "date_future"]
# Certains clients écrivent leurs coordonnées dans la description : le pipeline doit les masquer
COORDONNEES = [" Rappelez-moi au 06 12 34 56 78.", " Mon mail : jean.dupont@example.org", " Tel 0698765432 svp"]


class Simulateur:
    def __init__(self, cfg: Config, taux_erreur: float, graine: int | None = None):
        self.rng = random.Random(graine)
        self.taux_erreur = taux_erreur
        self.producteur = Producer({"bootstrap.servers": cfg.kafka_bootstrap, "linger.ms": 20, "enable.idempotence": True})
        self.dernier: tuple[str, bytes, bytes] | None = None
        self.ids_envoyes: list[str] = []  # utilisé par le banc d'essai pour ne compter que ses événements
        self.cfg = cfg
        self._charger_references(cfg)

    def _charger_references(self, cfg: Config) -> None:
        with connecter(cfg) as conn:
            self.logements = conn.execute(
                "SELECT l.id, l.client_id FROM app.logements l JOIN app.clients c ON c.id = l.client_id WHERE c.statut = 'actif'"
            ).fetchall()
            self.artisans = [r[0] for r in conn.execute("SELECT id FROM app.artisans WHERE statut = 'actif'")]
            self.interventions = [r[0] for r in conn.execute("SELECT id FROM app.interventions WHERE statut IN ('planifiee', 'en_cours')")]
            self.demandes = [r[0] for r in conn.execute("SELECT id FROM app.demandes ORDER BY cree_le DESC LIMIT 2000")]
        log.info(
            "Références : %d logements, %d artisans, %d interventions", len(self.logements), len(self.artisans), len(self.interventions)
        )

    # -- Construction des événements valides -------------------------------------
    def _enveloppe(self, type_: str) -> dict:
        return {"event_id": str(uuid.uuid4()), "type": type_, "emis_le": dt.datetime.now(dt.UTC).isoformat(), "version": 1}

    def _demande(self) -> tuple[dict, str]:
        logement_id, client_id = self.rng.choice(self.logements)
        metiers = poids_metiers(dt.date.today().month)
        metier = self.rng.choices(list(metiers), weights=list(metiers.values()))[0]
        description, _ = generer_description(metier, self.rng)
        if self.rng.random() < 0.05:
            description += self.rng.choice(COORDONNEES)
        demande_id = str(uuid.uuid4())
        self.demandes.append(demande_id)
        evt = self._enveloppe("demande.creee") | {
            "demande_id": demande_id,
            "client_id": str(client_id),
            "logement_id": str(logement_id),
            "description": description,
            "canal": self.rng.choices(["app", "web", "vocal", "telephone"], weights=[55, 30, 8, 7])[0],
        }
        return evt, demande_id

    def _message(self) -> tuple[dict, str]:
        demande_id = str(self.rng.choice(self.demandes[-500:]))
        auteur = self.rng.choice(["client", "artisan", "support"])
        evt = self._enveloppe("message.envoye") | {
            "message_id": str(uuid.uuid4()),
            "demande_id": demande_id,
            "auteur_type": auteur,
            "contenu": self.rng.choice(
                [
                    "Je suis disponible demain matin.",
                    "Pouvez-vous m'envoyer une photo ?",
                    "J'arrive dans 20 minutes.",
                    "Merci pour votre réactivité !",
                ]
            ),
            "metadonnees": {"canal": "app", "lu": False, "pieces_jointes": self.rng.randint(0, 2)},
        }
        return evt, demande_id

    def _position(self) -> tuple[dict, str]:
        artisan_id = str(self.rng.choice(self.artisans))
        evt = self._enveloppe("artisan.disponibilite") | {
            "artisan_id": artisan_id,
            "latitude": round(48.85 + self.rng.uniform(-0.25, 0.25), 5),
            "longitude": round(2.35 + self.rng.uniform(-0.4, 0.4), 5),
            "disponible": self.rng.random() < 0.7,
        }
        return evt, artisan_id

    def _statut(self) -> tuple[dict, str]:
        intervention_id = str(self.rng.choice(self.interventions))
        evt = self._enveloppe("intervention.statut") | {
            "intervention_id": intervention_id,
            "statut": self.rng.choice(["en_cours", "terminee"]),
        }
        return evt, intervention_id

    # -- Défauts volontaires ----------------------------------------------------
    def _degrader(self, topic: str, evt: dict) -> bytes:
        defaut = self.rng.choice(DEFAUTS)
        if defaut == "json_casse":
            return json.dumps(evt).encode()[:-7]
        if defaut == "champ_manquant":
            evt.pop(self.rng.choice([k for k in evt if k not in ("type",)]))
        elif defaut == "valeur_interdite":
            cible = {
                "demandes.creees": ("canal", "pigeon voyageur"),
                "messages.envoyes": ("auteur_type", "inconnu"),
                "artisans.disponibilite": ("disponible", "peut-être"),
                "interventions.statut": ("statut", "perdue"),
            }[topic]
            evt[cible[0]] = cible[1]
        elif defaut == "reference_inconnue":
            for champ in ("client_id", "demande_id", "artisan_id", "intervention_id"):
                if champ in evt and not (topic == "demandes.creees" and champ == "demande_id"):
                    evt[champ] = str(uuid.uuid4())
                    break
        elif defaut == "gps_hors_zone" and topic == "artisans.disponibilite":
            evt["latitude"] = 43.3  # Marseille : hors Île-de-France
        elif defaut == "date_future":
            evt["emis_le"] = (dt.datetime.now(dt.UTC) + dt.timedelta(days=2)).isoformat()
        return json.dumps(evt).encode()

    def evenement(self) -> tuple[str, bytes, bytes]:
        """Construit le prochain événement : (topic, clé de partition, contenu)."""
        if self.dernier and self.rng.random() < self.taux_erreur / len(DEFAUTS):
            return self.dernier  # doublon : même event_id renvoyé (rejeu réseau, double clic...)
        # Un type d'événement sans référence disponible (ex. toutes les interventions terminées) est sauté
        disponibles = {t: p for t, p in POIDS_TYPES.items() if t != "interventions.statut" or self.interventions}
        topic = self.rng.choices(list(disponibles), weights=list(disponibles.values()))[0]
        construire = {
            "demandes.creees": self._demande,
            "messages.envoyes": self._message,
            "artisans.disponibilite": self._position,
            "interventions.statut": self._statut,
        }[topic]
        evt, cle = construire()
        contenu = self._degrader(topic, evt) if self.rng.random() < self.taux_erreur else json.dumps(evt).encode()
        # La clé de partition garantit l'ordre des événements d'une même entité
        self.dernier = (topic, cle.encode(), contenu)
        return self.dernier

    def envoyer(self, n: int | None, debit: float) -> int:
        envoyes, actif, debut = 0, True, time.monotonic()

        def stop(*_):
            nonlocal actif
            actif = False

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        while actif and (n is None or envoyes < n):
            topic, cle, contenu = self.evenement()
            self.producteur.produce(topic, key=cle, value=contenu)
            if self.taux_erreur == 0:
                self.ids_envoyes.append(json.loads(contenu)["event_id"])
            self.producteur.poll(0)
            envoyes += 1
            if debit > 0:
                attente = envoyes / debit - (time.monotonic() - debut)
                if attente > 0:
                    time.sleep(attente)
            if envoyes % 1000 == 0:
                log.info("%d événements envoyés (%.0f/s)", envoyes, envoyes / (time.monotonic() - debut))
            if n is None and envoyes % 5000 == 0:
                self._charger_references(self.cfg)  # mode continu : prend en compte les nouvelles données
        self.producteur.flush(30)
        return envoyes


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s — %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--debit", type=float, default=20.0, help="événements par seconde (0 = au maximum)")
    parser.add_argument("--taux-erreur", type=float, default=0.03, help="part d'événements volontairement défectueux")
    parser.add_argument("--rafale", type=int, default=None, help="envoie N événements au maximum de débit puis s'arrête")
    parser.add_argument("--graine", type=int, default=None)
    args = parser.parse_args()

    simulateur = Simulateur(Config(), args.taux_erreur, args.graine)
    if args.rafale:
        debut = time.monotonic()
        n = simulateur.envoyer(args.rafale, debit=0)
        log.info("Rafale terminée : %d événements en %.1f s", n, time.monotonic() - debut)
    else:
        simulateur.envoyer(None, args.debit)
    return 0


if __name__ == "__main__":
    sys.exit(main())
