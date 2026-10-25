"""Client S3 (identité "pipeline" : buckets photos et datasets uniquement)."""

from __future__ import annotations

import boto3
from botocore.config import Config as BotoConfig

from artelio_pipeline.config import Config


def client_s3(cfg: Config):
    return boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint,
        aws_access_key_id=cfg.s3_access_key,
        aws_secret_access_key=cfg.s3_secret_key,
        region_name="eu-west-3",
        config=BotoConfig(retries={"max_attempts": 5, "mode": "standard"}, s3={"addressing_style": "path"}),
    )
