import pytest

@pytest.fixture(autouse=True)
def isolated_processing_mode(monkeypatch):
    monkeypatch.setenv("PROCESSING_MODE", "replay")
