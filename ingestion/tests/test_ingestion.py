"""
Testes unitários da ingestão.

Usamos pytest-mock para simular (mockar) as chamadas HTTP e GCS,
assim os testes rodam rápido e sem depender de internet ou credenciais.

Conceito de mock: substituímos a função real por uma função falsa
que retorna dados controlados — assim testamos só a nossa lógica.
"""

import pytest
from unittest.mock import MagicMock, patch

from ingestion.main import is_race_finished, get_config
from ingestion.extractors.openf1 import get_sessions
from ingestion.loaders.gcs import _build_blob_path


# ─── Testes de is_race_finished ───────────────────────────────────────────────

def test_race_finished_returns_true_for_past_date():
    race = {"date": "2020-03-15"}
    assert is_race_finished(race) is True


def test_race_finished_returns_false_for_future_date():
    race = {"date": "2099-12-31"}
    assert is_race_finished(race) is False


def test_race_finished_returns_false_for_missing_date():
    race = {}
    assert is_race_finished(race) is False


# ─── Testes de _build_blob_path ───────────────────────────────────────────────

def test_blob_path_with_round():
    path = _build_blob_path("laps", 2025, 1)
    assert path.startswith("laps/year=2025/round=01/")
    assert path.endswith(".json")


def test_blob_path_without_round():
    path = _build_blob_path("calendar", 2025)
    assert path.startswith("calendar/year=2025/")
    assert "round=" not in path


def test_blob_path_round_zero_padded():
    path = _build_blob_path("laps", 2025, 9)
    assert "round=09" in path


# ─── Testes de get_config ─────────────────────────────────────────────────────

def test_get_config_raises_if_bucket_missing(monkeypatch):
    monkeypatch.delenv("GCP_BUCKET_NAME", raising=False)
    monkeypatch.setenv("F1_SEASON", "2025")
    with pytest.raises(ValueError, match="bucket_name"):
        get_config()


def test_get_config_returns_correct_values(monkeypatch):
    monkeypatch.setenv("GCP_BUCKET_NAME", "meu-bucket")
    monkeypatch.setenv("F1_SEASON", "2025")
    config = get_config()
    assert config["bucket_name"] == "meu-bucket"
    assert config["season"] == 2025


# ─── Testes de get_sessions (mock HTTP) ───────────────────────────────────────

def test_get_sessions_returns_list(requests_mock):
    """
    Mocka a chamada HTTP para a OpenF1 API e verifica que
    get_sessions() retorna a lista corretamente.
    """
    fake_response = [
        {"session_key": 1, "session_type": "Race", "year": 2025},
        {"session_key": 2, "session_type": "Qualifying", "year": 2025},
    ]
    requests_mock.get(
        "https://api.openf1.org/v1/sessions",
        json=fake_response,
    )
    result = get_sessions(2025)
    assert len(result) == 2
    assert result[0]["session_key"] == 1


def test_get_sessions_raises_on_http_error(requests_mock):
    """Verifica que erros HTTP são propagados corretamente."""
    requests_mock.get(
        "https://api.openf1.org/v1/sessions",
        status_code=500,
    )
    with pytest.raises(Exception):
        get_sessions(2025)
