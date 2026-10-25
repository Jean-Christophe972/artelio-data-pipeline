"""Processeur de flux temps réel : Kafka → contrôles → PostgreSQL.

Pour chaque lot de messages (jusqu'à TAILLE_LOT messages ou ATTENTE_LOT_S secondes) :
  1. validation du contrat de données (schéma, valeurs, dates)       → rejet en DLQ si invalide
  2. dédoublonnage par identifiant d'événement (pipeline.evenements_traites)
  3. contrôle d'intégrité référentielle (client, logement, demande, artisan existent)
  4. écriture en base dans UNE transaction
  5. envoi des rejets dans le topic dlq.erreurs (contenu masqué, raison en en-tête)
  6. validation des offsets Kafka UNIQUEMENT après le commit en base

Garanties : aucun événement perdu (offsets validés après l'écriture), aucun doublon en base
(idempotence par event_id). En cas d'erreur de base, le lot est rejoué avec attente
progressive ; si l'erreur persiste, le processus s'arrête et Docker le redémarre : il
reprend au dernier offset validé.

Plusieurs instances peuvent tourner en parallèle (même groupe de consommateurs) : Kafka
répartit les partitions entre elles, et redistribue celles d'une instance qui tombe.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import signal
import sys
import time
from dataclasses import dataclass, field

import psycopg
from confluent_kafka import Consumer, KafkaError, Message, Producer
from prometheus_client import start_http_server

from artelio_pipeline import metriques as m
from artelio_pipeline.config import TOPICS_ENTRANTS, Config
from artelio_pipeline.db import connecter
from artelio_pipeline.evenements import (
    DemandeCreee,
    DisponibiliteArtisan,
    Evenement,
    EvenementInvalide,
    MessageEnvoye,
    StatutIntervention,
    valider,
)
from artelio_pipeline.pii import masquer

log = logging.getLogger("stream_processor")

# Cycle de vie d'une intervention : les événements peuvent arriver dans le désordre
# (réseau mobile, renvoi) ; une transition impossible est rejetée au lieu d'abîmer la donnée.
TRANSITIONS = {
    "planifiee": {"en_cours", "terminee", "annulee"},
    "en_cours": {"terminee", "annulee"},
    "terminee": set(),
    "annulee": set(),
}


@dataclass
class Rejet:
    topic: str
    raison: str
    detail: str
    contenu: bytes | None
    partition: int = -1
    offset: int = -1


@dataclass
class Lot:
    """Résultat de la validation d'un lot : événements valides par topic et rejets."""

    valides: dict[str, list[Evenement]] = field(default_factory=dict)
    rejets: list[Rejet] = field(default_factory=list)

    def tous_valides(self) -> list[Evenement]:
        return [e for evts in self.valides.values() for e in evts]


def preparer_lot(messages: list[tuple[str, bytes | None, int, int]]) -> Lot:
    """Valide un lot de messages (topic, contenu, partition, offset). Fonction pure, testée unitairement.

    Les doublons à l'intérieur d'un même lot sont aussi éliminés ici.
    """
    lot = Lot()
    vus: set = set()
    for topic, contenu, partition, offset in messages:
        m.EVT_RECUS.labels(topic).inc()
        try:
            evenement = valider(topic, contenu)
        except EvenementInvalide as exc:
            lot.rejets.append(Rejet(topic, exc.raison, exc.detail, contenu, partition, offset))
            continue
        if evenement.event_id in vus:
            m.EVT_DOUBLONS.labels(topic).inc()
            continue
        vus.add(evenement.event_id)
        lot.valides.setdefault(topic, []).append(evenement)
    return lot


class Ecrivain:
    """Écrit un lot validé en base, dans une seule transaction."""

    def __init__(self, conn: psycopg.Connection):
        self.conn = conn
        self.rejets: list[Rejet] = []
        self.succes: list[tuple[str, list[Evenement]]] = []

    def ecrire(self, lot: Lot) -> None:
        # Rejets et métriques ne sont publiés qu'après le COMMIT : si la transaction échoue
        # et que le lot est rejoué, rien n'est compté deux fois.
        self.rejets, self.succes = [], []
        with self.conn.transaction():
            nouveaux = self._dedoublonner(lot)
            self._demandes([e for e in lot.valides.get("demandes.creees", []) if e.event_id in nouveaux], lot)
            self._messages([e for e in lot.valides.get("messages.envoyes", []) if e.event_id in nouveaux], lot)
            self._positions([e for e in lot.valides.get("artisans.disponibilite", []) if e.event_id in nouveaux], lot)
            self._statuts([e for e in lot.valides.get("interventions.statut", []) if e.event_id in nouveaux], lot)
        lot.rejets.extend(self.rejets)
        for topic, evts in self.succes:
            self._publier_succes(topic, evts)

    # -- Étape 2 : idempotence --------------------------------------------------
    def _dedoublonner(self, lot: Lot) -> set:
        evenements = lot.tous_valides()
        if not evenements:
            return set()
        topics = {e.event_id: t for t, evts in lot.valides.items() for e in evts}
        rows = self.conn.execute(
            """INSERT INTO pipeline.evenements_traites (event_id, topic)
               SELECT * FROM unnest(%s::uuid[], %s::text[])
               ON CONFLICT (event_id) DO NOTHING RETURNING event_id""",
            ([e.event_id for e in evenements], [topics[e.event_id] for e in evenements]),
        ).fetchall()
        nouveaux = {r[0] for r in rows}
        for e in evenements:
            if e.event_id not in nouveaux:
                m.EVT_DOUBLONS.labels(topics[e.event_id]).inc()
        return nouveaux

    def _existants(self, sql: str, ids: list) -> set:
        if not ids:
            return set()
        return {r[0] for r in self.conn.execute(sql, (ids,)).fetchall()}

    def _rejeter(self, lot: Lot, topic: str, evenement: Evenement, raison: str, detail: str) -> None:
        self.rejets.append(Rejet(topic, raison, detail, evenement.model_dump_json().encode()))

    # -- Étapes 3 et 4 : intégrité référentielle puis écriture ----------------------
    def _demandes(self, evts: list[DemandeCreee], lot: Lot) -> None:
        if not evts:
            return
        rows = self.conn.execute(
            "SELECT l.id, l.client_id FROM app.logements l JOIN app.clients c ON c.id = l.client_id "
            "WHERE l.id = ANY(%s) AND c.statut = 'actif'",
            ([e.logement_id for e in evts],),
        ).fetchall()
        proprietaires = {r[0]: r[1] for r in rows}
        ok = []
        for e in evts:
            if proprietaires.get(e.logement_id) != e.client_id:
                self._rejeter(lot, "demandes.creees", e, "reference_inconnue", "client ou logement inconnu, ou logement d'un autre client")
            else:
                ok.append(e)
        with self.conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO app.demandes (id, client_id, logement_id, description, canal, statut, cree_le)
                   VALUES (%s, %s, %s, %s, %s, 'nouvelle', %s) ON CONFLICT (id) DO NOTHING""",
                [(e.demande_id, e.client_id, e.logement_id, e.description, e.canal, e.emis_le) for e in ok],
            )
        self._succes("demandes.creees", ok)

    def _messages(self, evts: list[MessageEnvoye], lot: Lot) -> None:
        if not evts:
            return
        existantes = self._existants("SELECT id FROM app.demandes WHERE id = ANY(%s)", [e.demande_id for e in evts])
        ok = []
        for e in evts:
            if e.demande_id not in existantes:
                self._rejeter(lot, "messages.envoyes", e, "reference_inconnue", "demande inconnue")
            else:
                ok.append(e)
        with self.conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO app.messages (id, demande_id, auteur_type, auteur_id, contenu, metadonnees, envoye_le)
                   VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING""",
                [(e.message_id, e.demande_id, e.auteur_type, e.auteur_id, e.contenu, json.dumps(e.metadonnees), e.emis_le) for e in ok],
            )
        self._succes("messages.envoyes", ok)

    def _positions(self, evts: list[DisponibiliteArtisan], lot: Lot) -> None:
        if not evts:
            return
        actifs = self._existants("SELECT id FROM app.artisans WHERE id = ANY(%s) AND statut = 'actif'", [e.artisan_id for e in evts])
        ok = []
        for e in evts:
            if e.artisan_id not in actifs:
                self._rejeter(lot, "artisans.disponibilite", e, "reference_inconnue", "artisan inconnu ou inactif")
            else:
                ok.append(e)
        with self.conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO app.positions_artisans (artisan_id, latitude, longitude, enregistree_le) VALUES (%s, %s, %s, %s)",
                [(e.artisan_id, e.latitude, e.longitude, e.emis_le) for e in ok],
            )
        self._succes("artisans.disponibilite", ok)

    def _statuts(self, evts: list[StatutIntervention], lot: Lot) -> None:
        if not evts:
            return
        rows = self.conn.execute(
            "SELECT id, statut FROM app.interventions WHERE id = ANY(%s) FOR UPDATE",
            ([e.intervention_id for e in evts],),
        ).fetchall()
        etat = {r[0]: r[1] for r in rows}
        ok = []
        for e in sorted(evts, key=lambda evt: evt.emis_le):
            actuel = etat.get(e.intervention_id)
            if actuel is None:
                self._rejeter(lot, "interventions.statut", e, "reference_inconnue", "intervention inconnue")
            elif e.statut not in TRANSITIONS[actuel]:
                self._rejeter(lot, "interventions.statut", e, "transition_invalide", f"{actuel} → {e.statut} interdit")
            else:
                etat[e.intervention_id] = e.statut
                ok.append(e)
        with self.conn.cursor() as cur:
            cur.executemany(
                """UPDATE app.interventions SET statut = %(statut)s,
                       debut_reel = CASE WHEN %(statut)s = 'en_cours' THEN coalesce(debut_reel, %(t)s) ELSE debut_reel END,
                       fin_reelle = CASE WHEN %(statut)s = 'terminee' THEN greatest(%(t)s, debut_reel) ELSE fin_reelle END
                   WHERE id = %(id)s""",
                [{"statut": e.statut, "t": e.emis_le, "id": e.intervention_id} for e in ok],
            )
        self._succes("interventions.statut", ok)

    def _succes(self, topic: str, evts: list[Evenement]) -> None:
        self.succes.append((topic, evts))

    @staticmethod
    def _publier_succes(topic: str, evts: list[Evenement]) -> None:
        if not evts:
            return
        m.EVT_TRAITES.labels(topic).inc(len(evts))
        maintenant = dt.datetime.now(dt.UTC)
        for e in evts:
            m.LATENCE.observe(max((maintenant - e.emis_le).total_seconds(), 0))


def envoyer_en_dlq(producteur: Producer, topic_dlq: str, rejets: list[Rejet]) -> None:
    """Publie les rejets dans la file d'erreurs. Le contenu est masqué (minimisation)."""
    for r in rejets:
        m.EVT_REJETES.labels(r.topic, r.raison).inc()
        texte = (r.contenu or b"").decode("utf-8", errors="replace")[:4000]
        texte_masque, _ = masquer(texte)
        producteur.produce(
            topic_dlq,
            value=texte_masque.encode(),
            headers={
                "topic_origine": r.topic,
                "raison": r.raison,
                "detail": r.detail[:500],
                "partition": str(r.partition),
                "offset": str(r.offset),
                "rejete_le": dt.datetime.now(dt.UTC).isoformat(),
            },
        )
    producteur.flush(10)


class Processeur:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.actif = True
        self.consommateur = Consumer(
            {
                "bootstrap.servers": cfg.kafka_bootstrap,
                "group.id": cfg.kafka_group,
                "enable.auto.commit": False,  # validation manuelle, après l'écriture en base
                "auto.offset.reset": "earliest",
                "partition.assignment.strategy": "cooperative-sticky",
            }
        )
        self.producteur = Producer({"bootstrap.servers": cfg.kafka_bootstrap, "enable.idempotence": True})
        self.conn = connecter(cfg)

    def arreter(self, *_):
        log.info("Signal d'arrêt reçu : fin du lot en cours puis arrêt propre")
        self.actif = False

    def lancer(self) -> None:
        self.consommateur.subscribe(TOPICS_ENTRANTS)
        log.info("Abonné à %s (groupe %s)", ", ".join(TOPICS_ENTRANTS), self.cfg.kafka_group)
        while self.actif:
            messages = self.consommateur.consume(num_messages=self.cfg.taille_lot, timeout=self.cfg.attente_lot_s)
            utiles = [msg for msg in messages if self._utile(msg)]
            if not utiles:
                continue
            debut = time.monotonic()
            lot = preparer_lot([(msg.topic(), msg.value(), msg.partition(), msg.offset()) for msg in utiles])
            self._ecrire_avec_reprise(lot)
            envoyer_en_dlq(self.producteur, self.cfg.topic_dlq, lot.rejets)
            self.consommateur.commit(asynchronous=False)
            m.TAILLE_LOT.observe(len(utiles))
            m.DUREE_LOT.observe(time.monotonic() - debut)
        self.consommateur.close()
        self.conn.close()
        log.info("Processeur arrêté proprement")

    @staticmethod
    def _utile(msg: Message) -> bool:
        if msg.error() is None:
            return True
        if msg.error().code() != KafkaError._PARTITION_EOF:
            log.error("Erreur Kafka : %s", msg.error())
        return False

    def _ecrire_avec_reprise(self, lot: Lot, tentatives: int = 6) -> None:
        for essai in range(1, tentatives + 1):
            try:
                Ecrivain(self.conn).ecrire(lot)
                return
            except (psycopg.errors.IntegrityError, psycopg.errors.DataError) as exc:
                # Erreur de DONNÉE (contrainte violée) : la rejouer ne sert à rien.
                # On isole la ou les lignes fautives au lieu de bloquer tout le flux.
                log.warning("Lot refusé par une contrainte de la base, traitement ligne par ligne : %s", exc.diag.message_primary)
                self._ecrire_ligne_par_ligne(lot)
                return
            except psycopg.Error as exc:
                m.ERREURS_BASE.inc()
                log.warning("Écriture du lot en échec (%s/%s) : %s", essai, tentatives, exc)
                if essai == tentatives:
                    raise  # Docker redémarre le conteneur, qui reprendra au dernier offset validé
                time.sleep(min(2**essai, 30))
                if self.conn.closed or self.conn.broken:
                    self.conn = connecter(self.cfg)

    def _ecrire_ligne_par_ligne(self, lot: Lot) -> None:
        """Chaque événement dans sa propre transaction ; ceux que la base refuse partent en DLQ."""
        for topic, evenements in lot.valides.items():
            for evenement in evenements:
                unitaire = Lot(valides={topic: [evenement]})
                try:
                    Ecrivain(self.conn).ecrire(unitaire)
                    lot.rejets.extend(unitaire.rejets)
                except (psycopg.errors.IntegrityError, psycopg.errors.DataError) as exc:
                    lot.rejets.append(
                        Rejet(topic, "contrainte_base", exc.diag.message_primary or str(exc), evenement.model_dump_json().encode())
                    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s — %(message)s")
    cfg = Config()
    start_http_server(cfg.metrics_port)
    processeur = Processeur(cfg)
    signal.signal(signal.SIGTERM, processeur.arreter)
    signal.signal(signal.SIGINT, processeur.arreter)
    processeur.lancer()
    return 0


if __name__ == "__main__":
    sys.exit(main())
