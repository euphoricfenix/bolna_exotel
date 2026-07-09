FROM python:3.10.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libgomp1 \
    git \
    ffmpeg \
    gcc \
    g++ \
    python3-dev \
    build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip and install wheel
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip setuptools wheel

# Install uvicorn first
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install uvicorn

# Install the bolna package from local source (this repo), not from PyPI/GitHub —
# keeps whatever local fixes are checked in actually running in the container.
COPY bolna /app/bolna_src/bolna
COPY pyproject.toml requirements.txt README.md LICENSE /app/bolna_src/
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install /app/bolna_src || \
    (echo "Failed to install bolna package. See error above." && exit 1)

# Copy application files
COPY local_setup/quickstart_server.py /app/
COPY local_setup/presets /app/presets

EXPOSE 5001

CMD ["uvicorn", "quickstart_server:app", "--host", "0.0.0.0", "--port", "5001"]
