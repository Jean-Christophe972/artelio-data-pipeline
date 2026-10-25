"""Export du jeu d'entraînement du modèle IA (bloc 4) : demandes étiquetées, pseudonymisées.

Étapes :
  1. extraction des demandes qualifiées (métier validé, urgence), hors clients effacés et hors
     clients qui se sont OPPOSÉS à l'entraînement de l'IA (droit d'opposition, RGPD art. 21) ;
  2. masquage des données personnelles écrites dans les textes (téléphones, emails, IBAN) ;
  3. barrière qualité : volume minimal, toutes les classes représentées, aucune donnée
     personnelle résiduelle, pas de doublon → sinon RIEN n'est publié ;
  4. publication versionnée dans S3 (datasets/demandes/<version>/) avec un manifeste
     (empreinte SHA-256, distribution des classes) ; "latest.json" pointe vers la dernière version.
Le bloc 4 entraîne toujours sur une version identifiée : l'entraînement est reproductible.
"""

from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import json
import logging
from collections import Counter
from dataclasses import dataclass

from artelio_pipeline.config import Config
from artelio_pipeline.db import connecter
from artelio_pipeline.pii import contient_donnees_personnelles, masquer
from artelio_pipeline.stockage import client_s3

log = logging.getLogger(__name__)

COLONNES = ["demande_pseudo", "texte", "metier", "urgence", "canal", "mois", "cree_le"]
METIERS = {"plomberie", "electricite", "serrurerie", "chauffage", "peinture", "menuiserie", "petits_travaux"}
URGENCES = {"faible", "normale", "haute", "critique"}

EXTRACTION = """
WITH derniere_opposition AS (
    SELECT DISTINCT ON (client_id) client_id, accorde
    FROM app.consentements WHERE finalite = 'entrainement_ia'
    ORDER BY client_id, horodatage DESC
)
SELECT dwh.pseudo(d.id), d.description, coalesce(d.metier_valide, d.metier_predit), d.urgence_predite, d.canal,
       extract(month FROM d.cree_le)::int, d.cree_le::date
FROM app.demandes d
JOIN app.clients c ON c.id = d.client_id AND c.statut = 'actif'
LEFT JOIN derniere_opposition o ON o.client_id = d.client_id
WHERE d.statut <> 'nouvelle'
  AND coalesce(d.metier_valide, d.metier_predit) IS NOT NULL
  AND d.urgence_predite IS NOT NULL
  AND coalesce(o.accorde, true)
ORDER BY d.cree_le
"""


class DatasetRefuse(Exception):
    """Le jeu de données ne passe pas la barrière qualité : il n'est pas publié."""


@dataclass
class Rapport:
    lignes: int
    exclues_opposition: int
    textes_masques: int
    metiers: dict[str, int]
    urgences: dict[str, int]


def valider_dataset(lignes: list[dict], minimum: int = 1000, minimum_par_classe: int = 30) -> None:
    """Barrière qualité. Fonction pure (testée) : lève DatasetRefuse avec la liste des problèmes."""
    problemes = []
    if len(lignes) < minimum:
        problemes.append(f"volume insuffisant ({len(lignes)} < {minimum})")
    metiers = Counter(r["metier"] for r in lignes)
    for m in METIERS:
        if metiers.get(m, 0) < minimum_par_classe:
            problemes.append(f"classe '{m}' sous-représentée ({metiers.get(m, 0)} exemples)")
    if inconnues := set(metiers) - METIERS:
        problemes.append(f"métiers inconnus : {sorted(inconnues)}")
    if inconnues := {r["urgence"] for r in lignes} - URGENCES:
        problemes.append(f"urgences inconnues : {sorted(inconnues)}")
    if any(contient_donnees_personnelles(r["texte"]) for r in lignes):
        problemes.append("données personnelles résiduelles dans les textes")
    if len({r["demande_pseudo"] for r in lignes}) != len(lignes):
        problemes.append("doublons de demandes")
    if problemes:
        raise DatasetRefuse(" ; ".join(problemes))


def extraire(cfg: Config) -> tuple[list[dict], Rapport]:
    with connecter(cfg) as conn:
        rows = conn.execute(EXTRACTION).fetchall()
        exclues = conn.execute(
            """SELECT count(*) FROM app.demandes d WHERE d.statut <> 'nouvelle' AND EXISTS (
                   SELECT 1 FROM app.consentements k WHERE k.client_id = d.client_id
                   AND k.finalite = 'entrainement_ia' AND NOT k.accorde)"""
        ).fetchone()[0]
    lignes, masques = [], 0
    for pseudo, texte, metier, urgence, canal, mois, cree_le in rows:
        texte_masque, n = masquer(texte)
        masques += n > 0
        lignes.append(
            {
                "demande_pseudo": pseudo,
                "texte": texte_masque,
                "metier": metier,
                "urgence": urgence,
                "canal": canal,
                "mois": mois,
                "cree_le": cree_le.isoformat(),
            }
        )
    rapport = Rapport(
        len(lignes), exclues, masques, dict(Counter(r["metier"] for r in lignes)), dict(Counter(r["urgence"] for r in lignes))
    )
    return lignes, rapport


def publier(cfg: Config, lignes: list[dict], rapport: Rapport) -> dict:
    valider_dataset(lignes)
    tampon = io.StringIO()
    writer = csv.DictWriter(tampon, fieldnames=COLONNES)
    writer.writeheader()
    writer.writerows(lignes)
    contenu = tampon.getvalue().encode("utf-8")
    empreinte = hashlib.sha256(contenu).hexdigest()
    version = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    prefixe = f"demandes/{version}"

    manifest = {
        "version": version,
        "fichier": f"{prefixe}/data.csv",
        "sha256": empreinte,
        "lignes": rapport.lignes,
        "colonnes": COLONNES,
        "periode": [lignes[0]["cree_le"], lignes[-1]["cree_le"]],
        "distribution_metiers": rapport.metiers,
        "distribution_urgences": rapport.urgences,
        "exclusions": {"opposition_entrainement_ia": rapport.exclues_opposition, "clients_effaces": "exclus par construction"},
        "textes_avec_donnees_masquees": rapport.textes_masques,
        "pseudonymisation": "HMAC-SHA256 des identifiants (clé détenue par le pipeline)",
    }
    s3 = client_s3(cfg)
    s3.put_object(Bucket="datasets", Key=f"{prefixe}/data.csv", Body=contenu, ContentType="text/csv")
    s3.put_object(
        Bucket="datasets", Key=f"{prefixe}/manifest.json", Body=json.dumps(manifest, indent=2).encode(), ContentType="application/json"
    )
    # Publication atomique : le pointeur n'est mis à jour qu'une fois les fichiers en place
    s3.put_object(
        Bucket="datasets", Key="demandes/latest.json", Body=json.dumps({"version": version}).encode(), ContentType="application/json"
    )

    with connecter(cfg) as conn:
        conn.execute(
            "INSERT INTO pipeline.datasets (version, cle_s3, lignes, sha256, manifest) VALUES (%s, %s, %s, %s, %s)",
            (version, manifest["fichier"], rapport.lignes, empreinte, json.dumps(manifest)),
        )
        conn.commit()
    log.info("Jeu d'entraînement %s publié : %d lignes, sha256 %s…", version, rapport.lignes, empreinte[:12])
    return manifest
