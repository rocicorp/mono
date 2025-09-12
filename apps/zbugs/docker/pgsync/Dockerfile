FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y gcc python3-dev libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip install pgsync[opensearch]

WORKDIR /app

COPY schema.json /app/schema.json
COPY bootstrap.sh /app/bootstrap.sh
COPY index-mapping.json /app/index-mapping.json
RUN chmod +x /app/bootstrap.sh

CMD ["/app/bootstrap.sh"]