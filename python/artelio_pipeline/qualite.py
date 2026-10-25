"""Contrôles qualité des données, alignés sur le plan de gouvernance (bloc 1, §6).

Chaque règle est une requête SQL qui renvoie (total, en_erreur). Le taux de conformité
est comparé au seuil de la règle. Les résultats sont historisés dans
pipeline.resultats_qualite et publiés vers Prometheus : une règle non conforme déclenche
l'alerte ControleQualiteEnEchec (sans bloquer le pipeline : on signale, on ne supprime pas).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from artelio_pipeline.config import Config
from artelio_pipeline.db import connecter
from artelio_pipeline.metriques import pousser

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Regle:
    nom: str
    dimension: str
    seuil: float
    sql: str  # doit renvoyer une ligne : (total, en_erreur)
    description: str


REGLES: list[Regle] = [
    Regle(
        "demandes_completes",
        "exhaustivité",
        0.99,
        """
        SELECT count(*), count(*) FILTER (WHERE length(description) < 20 OR logement_id IS NULL)
        FROM app.demandes WHERE cree_le > now() - interval '30 days'""",
        "Une demande a une adresse (logement) et une description d'au moins 20 caractères",
    ),
    Regle(
        "adresses_valides",
        "exactitude",
        0.98,
        """
        SELECT count(*), count(*) FILTER (WHERE latitude IS NULL OR latitude NOT BETWEEN 48.1 AND 49.25
                                            OR longitude NOT BETWEEN 1.4 AND 3.6)
        FROM app.logements""",
        "Adresse géocodée et située en Île-de-France",
    ),
    Regle(
        "clients_sans_doublon",
        "unicité",
        0.995,
        """
        SELECT count(*), count(*) - count(DISTINCT (lower(nom), lower(prenom), telephone))
        FROM app.clients WHERE statut = 'actif'""",
        "Pas deux comptes actifs pour la même personne (nom, prénom, téléphone)",
    ),
    Regle(
        "devis_valides",
        "validité",
        0.995,
        """
        SELECT count(*), count(*) FILTER (WHERE montant_ht <= 0 OR repondu_le < cree_le)
        FROM app.devis""",
        "Montant positif, réponse postérieure à l'envoi",
    ),
    Regle(
        "assurances_a_jour",
        "fraîcheur",
        0.98,
        """
        SELECT count(*), count(*) FILTER (WHERE assurance_decennale_expire_le < current_date)
        FROM app.artisans WHERE statut = 'actif'""",
        "Un artisan actif a une assurance décennale en cours de validité",
    ),
    Regle(
        "flux_temps_reel_frais",
        "fraîcheur",
        1.0,
        """
        SELECT 1, CASE WHEN max(traite_le) > now() - interval '15 minutes' THEN 0 ELSE 1 END
        FROM pipeline.evenements_traites""",
        "Au moins un événement temps réel traité dans les 15 dernières minutes",
    ),
    Regle(
        "factures_coherentes",
        "cohérence",
        0.999,
        """
        SELECT count(*), count(*) FILTER (WHERE f.montant_ttc <> v.montant_ttc)
        FROM app.factures f JOIN app.interventions i ON i.id = f.intervention_id JOIN app.devis v ON v.id = i.devis_id""",
        "Le montant facturé est celui du devis accepté",
    ),
    Regle(
        "interventions_terminees_datees",
        "cohérence",
        1.0,
        """
        SELECT count(*), count(*) FILTER (WHERE fin_reelle IS NULL)
        FROM app.interventions WHERE statut = 'terminee'""",
        "Une intervention terminée a une date de fin",
    ),
    Regle(
        "entrepot_complet",
        "cohérence",
        0.99,
        """
        SELECT (SELECT count(*) FROM app.demandes WHERE cree_le < now() - interval '2 hours'),
               greatest((SELECT count(*) FROM app.demandes WHERE cree_le < now() - interval '2 hours')
                        - (SELECT count(*) FROM dwh.fait_demande), 0)""",
        "Toutes les demandes de plus de 2 h sont présentes dans l'entrepôt",
    ),
]


@dataclass(frozen=True)
class Resultat:
    regle: Regle
    total: int
    en_erreur: int

    @property
    def taux(self) -> float:
        return 1.0 if self.total == 0 else 1 - self.en_erreur / self.total

    @property
    def conforme(self) -> bool:
        return self.taux >= self.regle.seuil


def executer(cfg: Config, regles: list[Regle] = REGLES) -> list[Resultat]:
    resultats = []
    with connecter(cfg) as conn:
        for regle in regles:
            total, en_erreur = conn.execute(regle.sql).fetchone()
            r = Resultat(regle, int(total or 0), int(en_erreur or 0))
            resultats.append(r)
            conn.execute(
                """INSERT INTO pipeline.resultats_qualite (regle, dimension, total, en_erreur, taux_conformite, seuil, conforme)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (regle.nom, regle.dimension, r.total, r.en_erreur, round(r.taux, 5), regle.seuil, r.conforme),
            )
            log.log(
                logging.INFO if r.conforme else logging.WARNING,
                "%-30s %7.3f %% (seuil %.1f %%) %s",
                regle.nom,
                100 * r.taux,
                100 * regle.seuil,
                "OK" if r.conforme else "NON CONFORME",
            )

        # Détection d'opérations massives suspectes (procédure de violation, plan de gouvernance §11.1)
        anomalies = conn.execute("SELECT * FROM audit.detecter_anomalies()").fetchall()
        for a in anomalies:
            log.error("Opération massive suspecte : %s %s %s (%s lignes en 1 h)", *a)
        conn.commit()

    valeurs = {}
    for r in resultats:
        labels = (("regle", r.regle.nom), ("dimension", r.regle.dimension))
        valeurs[("artelio_qualite_taux_conformite", labels)] = r.taux
        valeurs[("artelio_qualite_regle_ok", labels)] = 1.0 if r.conforme else 0.0
    valeurs[("artelio_anomalies_audit", ())] = float(len(anomalies))
    pousser(cfg.pushgateway, "qualite_donnees", valeurs)
    return resultats
