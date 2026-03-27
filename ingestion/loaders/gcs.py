"""
Loader — envia os dados extraídos para o Google Cloud Storage (camada Bronze).

O GCS é nosso data lake raw. Salvamos tudo em formato JSON,
particionado por ano e por corrida (round), assim:

gs://SEU_BUCKET/
├── sessions/year=2025/data.json
├── drivers/year=2025/round=01/data.json
├── laps/year=2025/round=01/data.json
├── pit_stops/year=2025/round=01/data.json
├── race_results/year=2025/round=01/data.json
├── driver_standings/year=2025/data.json
└── circuits/year=2025/data.json

Por que JSON e não Parquet aqui?
- O Bronze deve ser fiel ao dado bruto da fonte
- JSON é o formato nativo das APIs
- A conversão para Parquet acontece no Databricks (Silver)
"""

import os
import json
from datetime import datetime, timezone

from google.cloud import storage
from loguru import logger


def _get_client() -> storage.Client:
    """
    Cria o cliente do GCS.
    Usa Application Default Credentials (ADC) automaticamente —
    não precisa passar chave, o google-cloud-storage encontra
    as credenciais configuradas via 'gcloud auth application-default login'.
    """
    project_id = os.getenv("data-pipeline-490304")
    return storage.Client(project=project_id)


def _build_blob_path(entity: str, year: int, round_number: int | None = None) -> str:
    """
    Monta o caminho do arquivo no bucket seguindo a convenção de particionamento.

    Exemplos:
    - sessions, year=2025           → sessions/year=2025/data.json
    - laps, year=2025, round=1      → laps/year=2025/round=01/data.json
    """
    # Adiciona timestamp de ingestão no nome para facilitar reprocessamento
    ingested_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    if round_number is not None:
        return f"{entity}/year={year}/round={round_number:02d}/{ingested_at}.json"
    else:
        return f"{entity}/year={year}/{ingested_at}.json"


def upload_json(
    bucket_name: str,
    data: list[dict],
    entity: str,
    year: int,
    round_number: int | None = None,
) -> str:
    """
    Serializa os dados como JSON e faz upload para o GCS.

    Args:
        bucket_name:  nome do bucket (ex: 'f1-pipeline-bronze-luisin')
        data:         lista de registros a salvar
        entity:       nome da entidade (ex: 'laps', 'pit_stops', 'race_results')
        year:         ano da temporada
        round_number: número da corrida (None para dados anuais)

    Returns:
        O caminho completo do blob criado no GCS (gs://bucket/path)
    """
    if not data:
        logger.warning(
            f"Nenhum dado para {entity} | year={year} round={round_number} — upload ignorado"
        )
        return ""

    client = _get_client()
    bucket = client.bucket(bucket_name)

    blob_path = _build_blob_path(entity, year, round_number)
    blob = bucket.blob(blob_path)

    # Serializa para JSON com indentação para facilitar inspeção manual
    json_content = json.dumps(data, ensure_ascii=False, indent=2)

    blob.upload_from_string(
        json_content,
        content_type="application/json",
    )

    full_path = f"gs://{bucket_name}/{blob_path}"
    logger.success(f"Upload concluído: {full_path} ({len(data)} registros)")
    return full_path
