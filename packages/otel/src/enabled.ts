export function otelEnabled() {
  return otelMetricsEnabled() || otelTracesEnabled() || otelLogsEnabled();
}

export function otelMetricsEnabled() {
  return (
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT ||
    process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT ||
    process.env.OTEL_METRICS_EXPORTER
  );
}

export function otelLogsEnabled() {
  return (
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT ||
    process.env.OTEL_EXPORTER_OTLP_LOGS_ENDPOINT ||
    process.env.OTEL_LOGS_EXPORTER
  );
}

export function otelTracesEnabled() {
  return (
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT ||
    process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT ||
    process.env.OTEL_TRACES_EXPORTER
  );
}
