export function otelEnabled() {
  return otelMetricsEnabled() || otelTracesEnabled() || otelLogsEnabled();
}

export function otelMetricsEnabled() {
  return (
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT ||
    process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
  );
}

export function otelLogsEnabled() {
  return (
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT ||
    process.env.OTEL_EXPORTER_OTLP_LOGS_ENDPOINT
  );
}

export function otelTracesEnabled() {
  return (
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT ||
    process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
  );
}
