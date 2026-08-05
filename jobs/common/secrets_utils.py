"""Thin wrapper around Secrets Manager for pulling runtime secrets (e.g. the
PII-hashing salt) so no secret value is ever hardcoded in job code or Glue
job parameters -- only the secret's name/ARN is passed as a parameter."""
from __future__ import annotations

import json

import boto3


def get_secret_string(secret_id: str, key: str | None = None) -> str:
    """Fetches a secret's value. If the secret is stored as a JSON object
    (as created by infrastructure/cloudformation/02-iam.yaml's
    GenerateSecretString), pass `key` to extract one field; otherwise the
    raw SecretString is returned."""
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_id)
    raw = response["SecretString"]
    if key is None:
        return raw
    return json.loads(raw)[key]
