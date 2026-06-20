import os

os.environ.setdefault("RATELIMIT_ENABLED", "false")

import pytest
from starlette.testclient import TestClient

from app.main import app


@pytest.fixture
def client():
    return TestClient(app)
