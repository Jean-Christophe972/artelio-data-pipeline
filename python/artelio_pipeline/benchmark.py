"""Mesure du débit du pipeline : N événements valides envoyés d'un coup, chronométrés jusqu'en base.

    python -m artelio_pipeline.benchmark --evenements 20000

Indicateur de la grille d'évaluation (critère 1.1) : temps de traitement et lignes par seconde.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time

from artelio_pipeline.config import Config
from artelio_pipeline.db import connecter
from artelio_pipeline.simulateur import Simulateur

log = logging.getLogger("benchmark")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s — %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evenements", type=int, default=20000)
    args = parser.parse_args()
    cfg = Config()

    simulateur = Simulateur(cfg, taux_erreur=0.0, graine=7)
    debut = time.monotonic()
    simulateur.envoyer(args.evenements, debit=0)
    envoi = time.monotonic() - debut
    log.info("%d événements envoyés à Kafka en %.1f s (%.0f/s)", args.evenements, envoi, args.evenements / envoi)

    with connecter(cfg, autocommit=True) as conn:
        while True:
            # Seuls les événements du banc d'essai sont comptés (le simulateur permanent continue en parallèle)
            traites = conn.execute(
                "SELECT count(*) FROM pipeline.evenements_traites WHERE event_id = ANY(%s::uuid[])", (simulateur.ids_envoyes,)
            ).fetchone()[0]
            if traites >= args.evenements or time.monotonic() - debut > 600:
                break
            time.sleep(0.5)
    total = time.monotonic() - debut
    print(f"\nRésultat : {traites} événements traités de bout en bout en {total:.1f} s → {traites / total:.0f} événements/s")
    print("(débit soutenu par les processeurs en parallèle ; objectif du cahier des charges : 200/s en pointe)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
