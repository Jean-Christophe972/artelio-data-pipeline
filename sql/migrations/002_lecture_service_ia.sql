-- Le service IA (bloc 4) lit le catalogue des jeux publiés : c'est l'ancre d'intégrité
-- (empreinte SHA-256 de référence) qu'il compare au fichier téléchargé depuis S3.
GRANT USAGE ON SCHEMA pipeline TO ml_service;
GRANT SELECT ON pipeline.datasets TO ml_service;
