"""Application automatique des durées de conservation (plan de gouvernance §5.2).

La base applique la purge (fonction rgpd.purger_donnees_expirees, bloc 2) et renvoie la liste
des photos devenues orphelines ; le pipeline les supprime du stockage objet.
Les identifiants d'événements déjà traités sont aussi purgés au-delà de la rétention Kafka.
"""

from __future__ import annotations

import logging

from artelio_pipeline.config import Config
from artelio_pipeline.db import connecter
from artelio_pipeline.stockage import client_s3

log = logging.getLogger(__name__)


def purger(cfg: Config) -> dict:
    with connecter(cfg) as conn:
        resultat = conn.execute("SELECT rgpd.purger_donnees_expirees()").fetchone()[0]
        # Au-delà de 8 jours, Kafka ne peut plus rejouer un événement : sa trace d'idempotence est inutile
        cur = conn.execute("DELETE FROM pipeline.evenements_traites WHERE traite_le < now() - interval '8 days'")
        resultat["traces_idempotence_purgees"] = cur.rowcount
        conn.commit()

    photos = resultat.pop("photos_a_supprimer", [])
    s3 = client_s3(cfg)
    # Suppression par paquets de 1 000 (limite de l'API S3 DeleteObjects)
    for i in range(0, len(photos), 1000):
        s3.delete_objects(Bucket="photos", Delete={"Objects": [{"Key": k} for k in photos[i : i + 1000]], "Quiet": True})
    resultat["photos_supprimees"] = len(photos)
    log.info("Purge RGPD : %s", resultat)
    return resultat
