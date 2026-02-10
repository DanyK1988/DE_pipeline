FROM python:3.11-slim

# --- System deps ---
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        curl && \
    rm -rf /var/lib/apt/lists/*

# --- Env ---
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PYSPARK_PYTHON=python3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY etl /app/etl

CMD ["python", "-m", "etl.main"]