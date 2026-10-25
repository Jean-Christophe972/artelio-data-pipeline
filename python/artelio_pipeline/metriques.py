"""Métriques Prometheus du pipeline.

- Processeur de flux (service long) : métriques exposées sur HTTP (:8000/metrics), collectées
  par Prometheus toutes les 15 s.
- Tâches Airflow (courtes) : métriques poussées vers la Pushgateway à la fin de la tâche.
"""

from __future__ import annotations

import logging
import time

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, push_to_gateway

log = logging.getLogger(__name__)

# --- Processeur de flux ------------------------------------------------------
EVT_RECUS = Counter("artelio_evenements_recus_total", "Événements lus dans Kafka", ["topic"])
EVT_TRAITES = Counter("artelio_evenements_traites_total", "Événements écrits en base", ["topic"])
EVT_REJETES = Counter("artelio_evenements_rejetes_total", "Événements envoyés en file d'erreurs (DLQ)", ["topic", "raison"])
EVT_DOUBLONS = Counter("artelio_evenements_doublons_total", "Événements déjà traités, ignorés", ["topic"])
ERREURS_BASE = Counter("artelio_erreurs_base_total", "Lots en échec côté base (rejoués automatiquement)")
LATENCE = Histogram(
    "artelio_traitement_latence_secondes",
    "Délai entre l'émission de l'événement et son enregistrement en base",
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300),
)
DUREE_LOT = Histogram("artelio_lot_duree_secondes", "Durée de traitement d'un lot", buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5))
TAILLE_LOT = Histogram("artelio_lot_taille", "Nombre de messages par lot", buckets=(1, 10, 50, 100, 250, 500, 1000))


def pousser(cfg_pushgateway: str, job: str, valeurs: dict[tuple[str, tuple[tuple[str, str], ...]], float], aide: str = "") -> None:
    """Pousse des jauges vers la Pushgateway. valeurs : {(nom, ((label, valeur), ...)): nombre}.

    Un échec de publication est journalisé mais n'interrompt jamais le traitement métier.
    """
    registre = CollectorRegistry()
    jauges: dict[str, Gauge] = {}
    for (nom, labels), nombre in valeurs.items():
        noms_labels = [k for k, _ in labels]
        if nom not in jauges:
            jauges[nom] = Gauge(nom, aide or nom, noms_labels, registry=registre)
        jauge = jauges[nom].labels(**dict(labels)) if labels else jauges[nom]
        jauge.set(nombre)
    try:
        push_to_gateway(cfg_pushgateway, job=job, registry=registre, timeout=5)
    except OSError as exc:
        log.warning("Pushgateway injoignable, métriques non publiées : %s", exc)


def maintenant() -> float:
    return time.time()
