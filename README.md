# Artelio — Pipelines de données temps réel

Pipelines de la marketplace (fictive) **Artelio** : ingestion d'événements temps réel depuis Kafka, contrôles de
qualité, chargement de PostgreSQL et de l'entrepôt en étoile, purge RGPD et publication des jeux d'entraînement
du modèle IA.

> Certification « Architecte en Intelligence Artificielle » — **Bloc 3 : concevoir et mettre en œuvre des pipelines
> de données**. S'appuie sur la plateforme du bloc 2 (`artelio-data-platform`). Données 100 % synthétiques.

**Architecture détaillée, gestion des erreurs, performances, qualité, RGPD : [docs/ARCHITECTURE_PIPELINE.md](docs/ARCHITECTURE_PIPELINE.md)**

## En bref

| Pipeline | Type | Fréquence | Rôle |
|---|---|---|---|
| `stream-processor` (×2) | Flux | continu | Kafka → validation → dédoublonnage → PostgreSQL ; rejets → `dlq.erreurs` |
| `artelio_dwh_incremental` | Lot (Airflow) | 15 min | Entrepôt en étoile : dimensions (dont SCD 2) et faits, en upsert |
| `artelio_qualite_donnees` | Lot (Airflow) | 30 min | 9 règles de qualité + détection d'opérations suspectes |
| `artelio_rgpd_purge` | Lot (Airflow) | 3 h | Durées de conservation : base + photos S3 |
| `artelio_dataset_ia` | Lot (Airflow) | 4 h | Jeu d'entraînement pseudonymisé, masqué, versionné (→ bloc 4) |

**Chiffres mesurés :** ~10 000 événements/s en pointe, latence p95 de 1 s en régime normal, aucune perte lors de
la panne d'un processeur.

## Démarrage

**Prérequis :** la plateforme du bloc 2 déployée (`./scripts/deploy.sh --seed`), Docker, Terraform.

```bash
./scripts/configure_env.sh     # récupère les secrets de la plateforme → .env (non versionné)
docker compose up -d --build   # migrations, 2 processeurs, simulateur, Airflow
```

| Interface | Adresse |
|---|---|
| Airflow (utilisateur `admin`, mot de passe : `./scripts/airflow_password.sh`) | http://localhost:18080 |
| Grafana → tableau « Pipelines temps réel Artelio » | http://localhost:13000 |
| Alertes | http://localhost:19093 |

## Démonstrations

| Commande | Ce qu'elle prouve |
|---|---|
| `./scripts/lancer_outil.sh benchmark --evenements 20000` | **Débit** : temps de traitement de bout en bout et événements par seconde |
| `./scripts/demo_redondance.sh` | **Redondance** : un processeur tué pendant une rafale, le second reprend, aucune perte |
| `./scripts/lancer_outil.sh simulateur --rafale 2000 --taux-erreur 0.3` | **Contrôle qualité** : 30 % d'événements défectueux → rejets classés par raison dans Grafana |
| Airflow → déclencher `artelio_qualite_donnees` | **Qualité** : résultats par règle dans Grafana et `pipeline.resultats_qualite` |

Consulter la file d'erreurs (raison du rejet en en-tête) :

```bash
docker exec artelio-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic dlq.erreurs --from-beginning --max-messages 5 --property print.headers=true
```

## Structure

```
artelio-data-pipeline/
├── dags/                     DAG Airflow (orchestration seulement : la logique est dans python/)
├── sql/
│   ├── migrations/           Tables du schéma "pipeline" (idempotence, traçabilité, qualité, datasets)
│   └── dwh/                  Chargement incrémental de l'entrepôt (dimensions, SCD 2, faits)
├── python/artelio_pipeline/
│   ├── evenements.py         Contrats de données (Pydantic)
│   ├── stream_processor.py   Processeur de flux Kafka → PostgreSQL
│   ├── simulateur.py         Générateur d'événements (avec défauts volontaires)
│   ├── dwh.py · qualite.py · rgpd.py · dataset.py   Logique des DAG
│   ├── pii.py                Masquage des données personnelles
│   ├── journal.py            Traçabilité des exécutions, alerte d'échec
│   └── metriques.py          Métriques Prometheus
├── tests/                    42 tests (pytest) : contrats, masquage, lots, qualité, dataset, DAG
├── docker/                   Images du pipeline et d'Airflow
├── scripts/                  Configuration, démonstrations
└── docker-compose.yml
```

## Qualité du code

```bash
pip install -e ".[dev]" && ruff check . && pytest      # 36 tests unitaires
```

Les tests des DAG s'exécutent dans l'image Airflow ; la CI GitHub Actions (`.github/workflows/ci.yml`) lance
l'analyse statique, les tests unitaires et les tests des DAG à chaque modification.
