FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# libwebp is required by Pillow for WebP encoding/decoding
RUN apt-get update && apt-get install -y --no-install-recommends \
    libwebp-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies from lockfile (no dev extras)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy application code
COPY app/ ./app/

EXPOSE 8080

CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
