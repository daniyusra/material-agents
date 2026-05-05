import os

# Disable rate limiting before the app module is imported so tests are not
# throttled when run repeatedly within the same minute.
os.environ.setdefault("RATELIMIT_ENABLED", "false")

import pytest
from starlette.testclient import TestClient

from app.main import app


@pytest.fixture
def client():
    return TestClient(app)
