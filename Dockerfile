FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH \
    PYSPARK_PYTHON=python \
    SPARK_LOCAL_IP=127.0.0.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

CMD ["python", "src/05_prueba_docker.py"]