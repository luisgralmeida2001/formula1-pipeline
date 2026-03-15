"""
Extrator da Ergast API.

Ergast é gratuita, sem autenticação e tem dados históricos desde 1950.
Documentação: https://ergast.com/mrd

Usamos ela para complementar a OpenF1 com dados mais estáveis:
- Calendário oficial da temporada
- Resultado final das corridas (classificação)
- Standings (campeonato de pilotos e construtores)
- Informações de circuitos

Formato da URL: https://ergast.com/api/f1/{year}/{endpoint}.json?limit=100
"""

import requests
from loguru import logger


BASE_URL = "https://ergast.com/api/f1"
REQUEST_TIMEOUT = 30

# A Ergast pagina os resultados — usamos limit alto para pegar tudo de uma vez
DEFAULT_LIMIT = 100


def _get(path: str, params: dict | None = None) -> dict:
    """
    Faz GET na Ergast API e retorna o campo MRData do JSON.
    A Ergast sempre encapsula a resposta dentro de {"MRData": {...}}.
    """
    url = f"{BASE_URL}/{path}.json"
    all_params = {"limit": DEFAULT_LIMIT}
    if params:
        all_params.update(params)

    logger.info(f"GET {url} | params: {all_params}")

    response = requests.get(url, params=all_params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    data = response.json()["MRData"]
    return data


def get_race_calendar(year: int) -> list[dict]:
    """
    Retorna o calendário completo de corridas de um ano.

    Exemplo de registro:
    {
        "round": "1",
        "raceName": "Bahrain Grand Prix",
        "date": "2025-03-16",
        "time": "15:00:00Z",
        "Circuit": {
            "circuitId": "bahrain",
            "circuitName": "Bahrain International Circuit",
            "Location": {"country": "Bahrain", "locality": "Sakhir"}
        }
    }
    """
    data = _get(f"{year}")
    races = data.get("RaceTable", {}).get("Races", [])
    logger.info(f"  → {len(races)} corridas no calendário {year}")
    return races


def get_race_results(year: int, round_number: int) -> list[dict]:
    """
    Retorna o resultado final de uma corrida (posição, pontos, tempo).

    Exemplo de registro:
    {
        "position": "1",
        "points": "25",
        "Driver": {"driverId": "max_verstappen", "familyName": "Verstappen"},
        "Constructor": {"constructorId": "red_bull", "name": "Red Bull"},
        "Time": {"time": "1:31:44.742"},
        "status": "Finished"
    }
    """
    data = _get(f"{year}/{round_number}/results")
    races = data.get("RaceTable", {}).get("Races", [])
    results = races[0].get("Results", []) if races else []
    logger.info(f"  → {len(results)} resultados para round {round_number}/{year}")
    return results


def get_driver_standings(year: int) -> list[dict]:
    """
    Retorna o campeonato de pilotos (standings) de um ano.

    Exemplo de registro:
    {
        "position": "1",
        "points": "575",
        "wins": "19",
        "Driver": {"driverId": "max_verstappen", "familyName": "Verstappen"},
        "Constructors": [{"name": "Red Bull"}]
    }
    """
    data = _get(f"{year}/driverStandings")
    standings_lists = data.get("StandingsTable", {}).get("StandingsLists", [])
    standings = standings_lists[0].get("DriverStandings", []) if standings_lists else []
    logger.info(f"  → {len(standings)} pilotos no campeonato {year}")
    return standings


def get_constructor_standings(year: int) -> list[dict]:
    """
    Retorna o campeonato de construtores de um ano.
    """
    data = _get(f"{year}/constructorStandings")
    standings_lists = data.get("StandingsTable", {}).get("StandingsLists", [])
    standings = standings_lists[0].get("ConstructorStandings", []) if standings_lists else []
    logger.info(f"  → {len(standings)} construtores no campeonato {year}")
    return standings


def get_circuits(year: int) -> list[dict]:
    """
    Retorna todos os circuitos de uma temporada.
    """
    data = _get(f"{year}/circuits")
    circuits = data.get("CircuitTable", {}).get("Circuits", [])
    logger.info(f"  → {len(circuits)} circuitos em {year}")
    return circuits
