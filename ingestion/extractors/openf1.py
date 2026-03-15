"""
Extrator da OpenF1 API.

OpenF1 é gratuita e sem autenticação.
Documentação: https://openf1.org

Endpoints que usamos:
- /sessions   → lista as sessões (corridas, classificações, treinos) por ano
- /drivers    → pilotos de cada sessão
- /laps       → voltas de cada sessão
- /pit        → pit stops de cada sessão
- /position   → posições ao longo da corrida
"""

import requests
from loguru import logger


BASE_URL = "https://api.openf1.org/v1"

# Timeout em segundos para cada requisição
REQUEST_TIMEOUT = 30


def _get(endpoint: str, params: dict) -> list[dict]:
    """
    Faz GET na OpenF1 API e retorna a lista de registros.
    Lança exceção se a requisição falhar.
    """
    url = f"{BASE_URL}/{endpoint}"
    logger.info(f"GET {url} | params: {params}")

    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()  # lança HTTPError se status != 2xx

    data = response.json()
    logger.info(f"  → {len(data)} registros retornados")
    return data


def get_sessions(year: int) -> list[dict]:
    """
    Retorna todas as sessões de um ano (corridas, quali, treinos livres).

    Exemplo de registro retornado:
    {
        "session_key": 9158,
        "session_name": "Race",
        "session_type": "Race",
        "date_start": "2025-03-16T15:00:00+00:00",
        "circuit_short_name": "Bahrain",
        "country_name": "Bahrain",
        "year": 2025
    }
    """
    return _get("sessions", {"year": year})


def get_drivers(session_key: int) -> list[dict]:
    """
    Retorna os pilotos de uma sessão específica.

    Exemplo de registro:
    {
        "driver_number": 1,
        "full_name": "Max VERSTAPPEN",
        "name_acronym": "VER",
        "team_name": "Red Bull Racing",
        "session_key": 9158
    }
    """
    return _get("drivers", {"session_key": session_key})


def get_laps(session_key: int) -> list[dict]:
    """
    Retorna todas as voltas de uma sessão.

    Exemplo de registro:
    {
        "session_key": 9158,
        "driver_number": 1,
        "lap_number": 1,
        "lap_duration": 98.234,
        "duration_sector_1": 30.1,
        "duration_sector_2": 35.2,
        "duration_sector_3": 32.9,
        "is_pit_out_lap": false
    }
    """
    return _get("laps", {"session_key": session_key})


def get_pit_stops(session_key: int) -> list[dict]:
    """
    Retorna os pit stops de uma sessão.

    Exemplo de registro:
    {
        "session_key": 9158,
        "driver_number": 1,
        "lap_number": 20,
        "pit_duration": 2.45
    }
    """
    return _get("pit", {"session_key": session_key})


def get_positions(session_key: int) -> list[dict]:
    """
    Retorna as posições dos pilotos ao longo da sessão.

    Exemplo de registro:
    {
        "session_key": 9158,
        "driver_number": 1,
        "position": 1,
        "date": "2025-03-16T15:05:00+00:00"
    }
    """
    return _get("position", {"session_key": session_key})
