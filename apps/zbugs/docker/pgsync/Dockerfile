FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y gcc python3-dev libpq-dev && \
    rm -rf /var/lib/apt/lists/*

RUN pip install pgsync[opensearch]

WORKDIR /app

COPY schema.json /app/schema.json

CMD ["pgsync", "-c", "/app/schema.json", "--daemon"]