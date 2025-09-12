OpenSearch service (port 9200):

- Configured with 512MB heap memory for development
- Security plugin disabled for easier local development
- Single-node cluster configuration
- Health check included

OpenSearch Dashboards (port 5601):

- Web UI for visualizing and managing OpenSearch
- Security disabled to match OpenSearch config
- Depends on OpenSearch service being healthy

To start everything, run from the docker directory:
docker compose up

Services will be available at:

- PostgreSQL Primary: localhost:6434
- PostgreSQL Replica: localhost:6435
- OpenSearch: localhost:9200
- OpenSearch Dashboards: localhost:5601
