# OTEL

To collect traces for local development:

1. Clone https://github.com/grafana/docker-otel-lgtm
2. `cd docker-otel-lgtm`
3. `./run-lgtm.sh`
4. Add `ZERO_LOG_TRACE_COLLECTOR = "http://localhost:4318/v1/traces"` to your `.env`
