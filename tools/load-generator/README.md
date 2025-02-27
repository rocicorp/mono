# Load Generator

This tool generates load against a Zero database by making random updates to specified tables and fields.

## Building the Docker Image

To build the Docker image for the load generator:
```bash
# From the root of the repository
docker build -t load-generator -f tools/load-generator/Dockerfile .
```

## Running the Load Generator

You can run the load generator using Docker with various environment variables to configure its behavior:

```bash
docker run -e ZERO_UPSTREAM_DB="postgresql://user:password@host.docker.internal:6434/postgres" \
  -e ZERO_QPS=20 \
  -e ZERO_PERTURB_TABLE="issue" \
  -e ZERO_PERTURB_KEY="id" \
  -e ZERO_PERTURB_BOOLS="open" \
  -e ZERO_PERTURB_INTS="modified" \
  load-generator:latest
```

## Configuration Options

The load generator accepts the following environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `ZERO_UPSTREAM_DB` | Connection string for the PostgreSQL database | Required |
| `ZERO_QPS` | Queries per second to generate | `20` |
| `ZERO_PERTURB_TABLE` | Table to modify | Required |
| `ZERO_PERTURB_KEY` | Primary key column name | Required |
| `ZERO_PERTURB_BOOLS` | Comma-separated list of boolean columns to toggle | Optional |
| `ZERO_PERTURB_INTS` | Comma-separated list of integer columns to increment | Optional |

## Examples

### Basic Usage

```bash
docker run -e ZERO_UPSTREAM_DB="postgresql://user:password@host.docker.internal:6434/postgres" \
  -e ZERO_PERTURB_TABLE="issue" \
  -e ZERO_PERTURB_KEY="id" \
  -e ZERO_PERTURB_BOOLS="open" \
  load-generator:latest
```

### Higher Load with Multiple Fields

```bash
docker run -e ZERO_UPSTREAM_DB="postgresql://user:password@host.docker.internal:6434/postgres" \
  -e ZERO_QPS=50 \
  -e ZERO_PERTURB_TABLE="issue" \
  -e ZERO_PERTURB_KEY="id" \
  -e ZERO_PERTURB_BOOLS="open,resolved" \
  -e ZERO_PERTURB_INTS="modified,priority" \
  load-generator:latest
```

## Notes

- When running against a local PostgreSQL instance, use `host.docker.internal` instead of `localhost` to access the host machine from within the Docker container.
- The load generator will continue running until manually stopped.
