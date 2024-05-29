#!/bin/bash
REPLICA_CONTAINER_ID=$(docker ps --filter "name=replica" --format "{{.ID}}")

if [ -z "$REPLICA_CONTAINER_ID" ]; then
    echo "No replica container found."
    exit 1
fi

until [ "$(docker inspect -f {{.State.Health.Status}} "$REPLICA_CONTAINER_ID")" == "healthy" ]; do
    echo "Waiting for postgres_replica to be healthy..."
    sleep 5
done

docker exec -u postgres "$REPLICA_CONTAINER_ID" psql -U user -d postgres -c "CREATE INDEX issuelabel_issueid_idx ON public.\"issueLabel\" (\"issueID\");"
docker exec -u postgres "$REPLICA_CONTAINER_ID" psql -U user -d postgres -c "CREATE INDEX issue_modified_idx ON public.issue (modified);"
docker exec -u postgres "$REPLICA_CONTAINER_ID" psql -U user -d postgres -c "CREATE INDEX issue_created_idx ON public.issue (created);"
docker exec -u postgres "$REPLICA_CONTAINER_ID" psql -U user -d postgres -c "CREATE INDEX issue_priority_modified_idx ON public.issue (priority,modified);"
docker exec -u postgres "$REPLICA_CONTAINER_ID" psql -U user -d postgres -c "CREATE INDEX issue_status_modified_idx ON public.issue (status,modified);"
docker exec -u postgres "$REPLICA_CONTAINER_ID" psql -U user -d postgres -c "CREATE INDEX comment_issueid_idx ON public.\"comment\" (\"issueID\");"