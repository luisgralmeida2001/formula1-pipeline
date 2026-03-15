"""
Ponto de entrada da ingestão F1.

O que esse script faz:
1. Lê as configurações do .env
2. Busca o calendário da temporada via Ergast API
3. Para cada corrida já realizada:
   - Busca dados da OpenF1 (voltas, pit stops, posições, pilotos)
   - Busca resultado final via Ergast
   - Sobe tudo para o GCS (Bronze)
4. Busca dados anuais (standings, circuitos) e sobe para o GCS

Rodando localmente:
    python -m ingestion.main

Rodando via Docker:
    docker compose up
"""

import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from loguru import logger

from ingestion.extractors import ergast, openf1
from ingestion.loaders.gcs import upload_json

# Carrega o .env automaticamente
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


def is_race_finished(race: dict) -> bool:
    """
    Verifica se uma corrida já aconteceu comparando a data com hoje (UTC).
    Só faz sentido buscar dados de corridas que já rolaram.
    """
    race_date_str = race.get("date", "")
    if not race_date_str:
        return False

    race_date = datetime.fromisoformat(race_date_str).replace(tzinfo=timezone.utc)
    return race_date < datetime.now(timezone.utc)


def ingest_race(bucket_name: str, year: int, race: dict) -> None:
    """
    Ingere todos os dados de uma corrida específica para o GCS.

    Para cada corrida buscamos:
    - OpenF1: pilotos, voltas, pit stops, posições
    - Ergast: resultado final (posição, pontos, tempo)
    """
    round_number = int(race["round"])
    race_name = race.get("raceName", f"Round {round_number}")
    logger.info(f"--- Ingerindo: {race_name} (round {round_number}) ---")

    # Precisamos do session_key da OpenF1 para buscar dados detalhados
    sessions = openf1.get_sessions(year)
    race_sessions = [
        s for s in sessions
        if s.get("session_type") == "Race" and s.get("meeting_key") is not None
    ]

    # Encontra a sessão correspondente a esse round pelo nome do circuito
    circuit_name = race.get("Circuit", {}).get("circuitId", "").lower()
    matching_session = next(
        (s for s in race_sessions if circuit_name in s.get("circuit_short_name", "").lower()),
        None
    )

    if matching_session:
        session_key = matching_session["session_key"]
        logger.info(f"  Session key OpenF1: {session_key}")

        drivers = openf1.get_drivers(session_key)
        upload_json(bucket_name, drivers, "drivers", year, round_number)

        laps = openf1.get_laps(session_key)
        upload_json(bucket_name, laps, "laps", year, round_number)

        pit_stops = openf1.get_pit_stops(session_key)
        upload_json(bucket_name, pit_stops, "pit_stops", year, round_number)

        positions = openf1.get_positions(session_key)
        upload_json(bucket_name, positions, "positions", year, round_number)
    else:
        logger.warning(f"  Sessão OpenF1 não encontrada para {race_name} — pulando dados detalhados")

    # Resultado final via Ergast
    results = ergast.get_race_results(year, round_number)
    upload_json(bucket_name, results, "race_results", year, round_number)


def ingest_season(bucket_name: str, year: int) -> None:
    """
    Ingere todos os dados anuais da temporada:
    - Calendário completo
    - Standings de pilotos e construtores
    - Circuitos
    - Dados de cada corrida já realizada
    """
    logger.info(f"========== Iniciando ingestão da temporada {year} ==========")

    calendar = ergast.get_race_calendar(year)
    upload_json(bucket_name, calendar, "calendar", year)

    driver_standings = ergast.get_driver_standings(year)
    upload_json(bucket_name, driver_standings, "driver_standings", year)

    constructor_standings = ergast.get_constructor_standings(year)
    upload_json(bucket_name, constructor_standings, "constructor_standings", year)

    circuits = ergast.get_circuits(year)
    upload_json(bucket_name, circuits, "circuits", year)

    sessions = openf1.get_sessions(year)
    upload_json(bucket_name, sessions, "sessions", year)

    finished_races = [r for r in calendar if is_race_finished(r)]
    logger.info(f"{len(finished_races)} corridas já realizadas em {year}")

    for race in finished_races:
        try:
            ingest_race(bucket_name, year, race)
        except Exception as e:
            logger.error(f"Erro ao ingerir {race.get('raceName')}: {e}")

    logger.success(f"========== Ingestão {year} concluída ==========")


def main() -> None:
    config = get_config()
    ingest_season(
        bucket_name=config["bucket_name"],
        year=config["season"],
    )


if __name__ == "__main__":
    main()
