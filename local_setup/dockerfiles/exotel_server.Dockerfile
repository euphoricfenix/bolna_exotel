FROM python:3.10.13-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \ 
    && apt-get clean \ 
    && rm -rf /var/lib/apt/lists/*

# Use the same requirements file you already have
COPY telephony_server/requirements.txt /app/requirements.txt

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt

# Exotel server + env
COPY telephony_server/exotel_api_server.py /app/
COPY telephony_server/.env /app/.env

EXPOSE 8003

# Launch Exotel server
CMD ["uvicorn", "exotel_api_server:app", "--host", "0.0.0.0", "--port", "8003"]
