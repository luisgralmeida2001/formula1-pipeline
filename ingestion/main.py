"""
Ponto de entrada da ingestão F1.

Usamos exclusivamente a OpenF1 API (gratuita, sem autenticação).
A Ergast API foi descontinuada em 2024.

Fluxo:
1. Busca todas as sessões da temporada
2. Sobe as sessões para o GCS (bronze/sessions)
3. Para cada corrida (session_type=Race) já realizada:
   - Busca pilotos, voltas, pit stops e posições
   - Sobe cada entidade particionada por round

Rodando localmente:
    python -m ingestion.main

Rodando via Docker:
    docker compose up
"""

import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from loguru import logger

from ingestion.extractors import openf1
from ingestion.loaders.gcs import upload_json

load_dotenv()


def get_config() -> dict:
    """Lê e valida as variáveis de ambiente necessárias."""
    config = {
        "bucket_name": os.getenv("GCP_BUCKET_NAME"),
        "season": int(os.getenv("F1_SEASON", "2025")),
    }
    missing = [k for k, v in config.items() if not v]
    if missing:
        raise ValueError(f"Variáveis de ambiente faltando: {missing}. Verifique o .env")
    return config


def is_session_finished(session: dict) -> bool:
    """
    Verifica se uma sessão já aconteceu comparando date_end com agora (UTC).
    A OpenF1 retorna date_end no formato ISO 8601.
    """
    date_end_str = session.get("date_end", "")
    if not date_end_str:
        return False
    try:
        date_end = datetime.fromisoformat(date_end_str.replace("Z", "+00:00"))
        return date_end < datetime.now(timezone.utc)
    except ValueError:
        return False


def ingest_race_session(bucket_name: str, year: int, session: dict) -> None:
    """
    Ingere todos os dados de uma sessão de corrida para o GCS.
    Busca: pilotos, voltas, pit stops e posições.
    """
    session_key = session["session_key"]
    round_number = session.get("meeting_key", 0)
    circuit = session.get("circuit_short_name", "unknown")

    logger.info(f"--- Ingerindo corrida: {circuit} (session_key={session_key}) ---")

    drivers = openf1.get_drivers(session_key)
    upload_json(bucket_name, drivers, "drivers", year, round_number)

    laps = openf1.get_laps(session_key)
    upload_json(bucket_name, laps, "laps", year, round_number)

    pit_stops = openf1.get_pit_stops(session_key)
    upload_json(bucket_name, pit_stops, "pit_stops", year, round_number)

    positions = openf1.get_positions(session_key)
    upload_json(bucket_name, positions, "positions", year, round_number)


def ingest_season(bucket_name: str, year: int) -> None:
    """
    Ingere todos os dados da temporada via OpenF1.
    """
    logger.info(f"========== Iniciando ingestão da temporada {year} ==========")

    # Busca e persiste todas as sessões do ano
    sessions = openf1.get_sessions(year)
    upload_json(bucket_name, sessions, "sessions", year)
    logger.info(f"{len(sessions)} sessões encontradas em {year}")

    # Filtra só as corridas (Race) que já aconteceram
    race_sessions = [
        s for s in sessions
        if s.get("session_type") == "Race" and is_session_finished(s)
    ]
    logger.info(f"{len(race_sessions)} corridas já realizadas em {year}")

    for session in race_sessions:
        try:
            ingest_race_session(bucket_name, year, session)
        except Exception as e:
            logger.error(f"Erro ao ingerir sessão {session.get('session_key')}: {e}")

    logger.success(f"========== Ingestão {year} concluída ==========")


def main() -> None:
    config = get_config()
    ingest_season(
        bucket_name=config["bucket_name"],
        year=config["season"],
    )


if __name__ == "__main__":
    main()
