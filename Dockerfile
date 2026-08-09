# Dockerfile.transform
#
# Runs the Bronze -> Silver PySpark transformation job.
# Place this file at the project root (next to docker-compose.yml).

FROM python:3.11-slim

# ------------------------------------------------------------
# System dependencies: Java (required by PySpark) + basic tools
# ------------------------------------------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        procps \
        curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ------------------------------------------------------------
# Python dependencies
# ------------------------------------------------------------
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ------------------------------------------------------------
# Project source
# ------------------------------------------------------------
COPY src/ ./src/

# .env, data/, etc. are mounted as volumes at runtime (see
# docker-compose.yml) rather than baked into the image, so the
# container always sees your current local data and config.

CMD ["python", "src/transformation/transform_weather.py"]