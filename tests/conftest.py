import os
import pytest
import httpx


@pytest.fixture(scope="session")
def base_url() -> str:
    return os.getenv("API_BASE_URL", "http://localhost:8000")


@pytest.fixture(scope="session")
def http_client(base_url: str) -> httpx.Client:
    with httpx.Client(base_url=base_url, timeout=10) as client:
        yield client


@pytest.fixture(scope="session")
def require_integration() -> None:
    if os.getenv("INTEGRATION") != "1":
        pytest.skip("Integration tests disabled. Set INTEGRATION=1 to enable.")
