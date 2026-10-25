"""Applique les migrations du schéma "pipeline" (lancé au démarrage par le service pipeline-init)."""

from __future__ import annotations

import logging
import sys

from artelio_pipeline.config import Config
from artelio_pipeline.db import appliquer_migrations


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s — %(message)s")
    appliquees = appliquer_migrations(Config())
    logging.getLogger("migrations").info("Migrations appliquées : %s", appliquees or "aucune (schéma à jour)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
