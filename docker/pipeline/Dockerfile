# Image des services du pipeline : processeur de flux, simulateur, migrations
FROM python:3.12-slim

RUN useradd --create-home --uid 10001 artelio
WORKDIR /app

# Dépendances épinglées d'abord (cache Docker), puis le paquet lui-même
COPY docker/pipeline/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY pyproject.toml ./
COPY python ./python
RUN pip install --no-cache-dir --no-deps .
COPY sql ./sql

ENV ARTELIO_SQL_DIR=/app/sql PYTHONUNBUFFERED=1
USER artelio
CMD ["python", "-m", "artelio_pipeline.stream_processor"]
