-- =============================================================================
-- Tables propres au pipeline (schéma "pipeline", créé par la plateforme du bloc 2)
-- =============================================================================

-- Idempotence : chaque événement Kafka a un identifiant unique. On l'enregistre dans la
-- même transaction que la donnée : un événement rejoué (redémarrage, doublon du producteur)
-- est détecté et ignoré → effet "exactement une fois" en base.
CREATE TABLE pipeline.evenements_traites (
    event_id   uuid PRIMARY KEY,
    topic      text NOT NULL,
    traite_le  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON pipeline.evenements_traites (traite_le);

-- Traçabilité (lineage) : chaque exécution de tâche, ce qu'elle a lu et écrit
CREATE TABLE pipeline.journal_executions (
    id              bigserial PRIMARY KEY,
    dag_id          text NOT NULL,
    run_id          text NOT NULL,
    tache           text NOT NULL,
    debut           timestamptz NOT NULL,
    fin             timestamptz,
    statut          text NOT NULL CHECK (statut IN ('en_cours', 'succes', 'echec')),
    lignes_lues     bigint,
    lignes_ecrites  bigint,
    details         jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX ON pipeline.journal_executions (dag_id, debut DESC);

-- Résultats des contrôles qualité (historisés : on suit la tendance, pas seulement l'instant)
CREATE TABLE pipeline.resultats_qualite (
    id               bigserial PRIMARY KEY,
    execute_le       timestamptz NOT NULL DEFAULT now(),
    regle            text NOT NULL,
    dimension        text NOT NULL,
    total            bigint NOT NULL,
    en_erreur        bigint NOT NULL,
    taux_conformite  numeric(6,5) NOT NULL,
    seuil            numeric(6,5) NOT NULL,
    conforme         boolean NOT NULL
);
CREATE INDEX ON pipeline.resultats_qualite (regle, execute_le DESC);

-- Points de reprise des chargements incrémentaux
CREATE TABLE pipeline.watermarks (
    nom     text PRIMARY KEY,
    valeur  timestamptz NOT NULL
);

-- Catalogue des jeux d'entraînement publiés (versionnés, pour la reproductibilité du bloc 4)
CREATE TABLE pipeline.datasets (
    version   text PRIMARY KEY,
    cle_s3    text NOT NULL,
    lignes    integer NOT NULL,
    sha256    char(64) NOT NULL,
    cree_le   timestamptz NOT NULL DEFAULT now(),
    manifest  jsonb NOT NULL
);

-- Le DPO et les analystes peuvent consulter la traçabilité (aucune donnée personnelle ici)
GRANT SELECT ON pipeline.journal_executions, pipeline.resultats_qualite, pipeline.datasets TO dpo_officer, analyst;
