# Airflow 2.11 + le paquet artelio_pipeline (logique métier testée hors Airflow)
FROM apache/airflow:2.11.0-python3.12

COPY --chown=airflow:root pyproject.toml /opt/artelio/
COPY --chown=airflow:root python /opt/artelio/python
# On réinstalle la même version d'Airflow en même temps pour qu'aucune dépendance ne la modifie
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" /opt/artelio

ENV ARTELIO_SQL_DIR=/opt/artelio/sql
