"""Chargement incrémental de l'entrepôt en étoile (schéma dwh).

Fenêtre glissante : on recharge tout ce qui a été créé depuis le dernier chargement réussi,
moins une marge de 30 jours (une demande ou une intervention continue d'évoluer après sa
création : devis, acceptation, facture, avis). Les écritures sont des "upserts" : rejouer
le chargement ne crée jamais de doublon (idempotence).
"""

from __future__ import annotations

import datetime as dt
import logging

from artelio_pipeline.config import Config
from artelio_pipeline.db import connecter, lire_sql
from artelio_pipeline.metriques import maintenant, pousser

log = logging.getLogger(__name__)
WATERMARK = "dwh_incremental"
MARGE = dt.timedelta(days=30)
ETAPES = ["dwh/01_dimensions.sql", "dwh/02_dim_artisan_scd2.sql", "dwh/03_fait_demande.sql", "dwh/04_fait_intervention.sql"]


def charger(cfg: Config) -> dict:
    """Exécute toutes les étapes dans UNE transaction : l'entrepôt n'est jamais à moitié chargé."""
    debut_chargement = dt.datetime.now(dt.UTC)
    resultat: dict = {"lignes": {}}
    with connecter(cfg) as conn:
        row = conn.execute("SELECT valeur FROM pipeline.watermarks WHERE nom = %s", (WATERMARK,)).fetchone()
        depuis = (row[0] - MARGE) if row else dt.datetime(2000, 1, 1, tzinfo=dt.UTC)  # premier chargement : tout
        resultat["depuis"] = depuis.isoformat()
        for etape in ETAPES:
            sql = lire_sql(etape)
            total = 0
            for requete in (r for r in sql.split(";\n") if r.strip() and not _commentaire_seul(r)):
                cur = conn.execute(requete, {"depuis": depuis} if "%(depuis)s" in requete else None)
                total += max(cur.rowcount, 0)
            resultat["lignes"][etape] = total
            log.info("%-32s %6d lignes", etape, total)
        conn.execute(
            "INSERT INTO pipeline.watermarks (nom, valeur) VALUES (%s, %s) ON CONFLICT (nom) DO UPDATE SET valeur = EXCLUDED.valeur",
            (WATERMARK, debut_chargement),
        )
        conn.commit()

    pousser(
        cfg.pushgateway,
        "dwh_incremental",
        {("artelio_dwh_dernier_chargement_timestamp_seconds", ()): maintenant()}
        | {("artelio_dwh_lignes_chargees", (("etape", e.split("/")[1].removesuffix(".sql")),)): n for e, n in resultat["lignes"].items()},
    )
    return resultat


def _commentaire_seul(bloc: str) -> bool:
    return all(not ligne.strip() or ligne.strip().startswith("--") for ligne in bloc.splitlines())
