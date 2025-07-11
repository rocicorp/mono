/* eslint-disable no-console */
import type {VercelRequest, VercelResponse} from '@vercel/node';

/**
 * Simple assertion function for validation
 */
function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message);
  }
}

/**
 * Validates that the JSON contains OTEL-related data
 */
function validateOtelData(data: unknown): boolean {
  if (!data || typeof data !== 'object') {
    return false;
  }

  const obj = data as Record<string, unknown>;

  // Check for common OTEL fields
  return !!(
    obj.resourceMetrics ||
    obj.resource ||
    obj.metrics ||
    obj.instrumentationScope ||
    obj.dataPoints ||
    (obj.resource && typeof obj.resource === 'object')
  );
}

/**
 * OTEL proxy handler that forwards metrics to Grafana OTLP endpoint.
 * Only accepts JSON requests and validates for OTEL data.
 */
export default async function handler(
  req: VercelRequest,
  res: VercelResponse,
): Promise<void> {
  console.log(`OTEL Proxy: ${req.method} request received`);

  if (req.method !== 'POST') {
    console.log('Rejecting non-POST request');
    res.status(405).json({error: 'Method not allowed'});
    return;
  }

  // Validate Content-Type is application/json
  const contentType = req.headers?.['content-type'] || '';
  if (!contentType.includes('application/json')) {
    console.log(`Rejecting non-JSON request. Content-Type: ${contentType}`);
    res.status(400).json({error: 'Invalid request type'});
    return;
  }

  const {ROCICORP_TELEMETRY_TOKEN, GRAFANA_OTLP_ENDPOINT} = process.env;
  const endpoint =
    GRAFANA_OTLP_ENDPOINT ||
    'https://otlp-gateway-prod-us-east-2.grafana.net/otlp/v1/metrics';

  console.log(
    `Token configured: ${!!ROCICORP_TELEMETRY_TOKEN}, Endpoint: ${endpoint}`,
  );
  console.log(`Incoming Content-Type: ${contentType}`);

  if (!ROCICORP_TELEMETRY_TOKEN) {
    console.error('ROCICORP_TELEMETRY_TOKEN not configured');
    res.status(500).json({error: 'Telemetry token not configured'});
    return;
  }

  try {
    // Validate request body exists
    assert(req.body, 'Request body is required for metrics forwarding');

    // Validate that the JSON contains OTEL-related data
    if (!validateOtelData(req.body)) {
      console.log('Rejecting request: No OTEL-related data found in JSON');
      res.status(400).json({error: 'Invalid request body'});
      return;
    }

    // Prepare JSON body for forwarding
    const bodyToSend =
      typeof req.body === 'string' ? req.body : JSON.stringify(req.body);

    console.log(`JSON body prepared, length: ${bodyToSend.length}`);
    console.log('Forwarding to Grafana...');

    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'authorization': `Bearer ${ROCICORP_TELEMETRY_TOKEN}`,
      },
      body: bodyToSend,
    });

    console.log(`Grafana response: ${response.status}`);
    res.status(response.status);

    if (response.headers.get('content-type')?.includes('application/json')) {
      const jsonResponse = await response.json();
      console.log('Grafana JSON response:', JSON.stringify(jsonResponse));
      res.json(jsonResponse);
      return;
    }

    const textResponse = await response.text();
    console.log('Grafana text response:', textResponse);
    res.send(textResponse);
  } catch (error) {
    console.error(
      'Error forwarding metrics:',
      error instanceof Error ? error.message : String(error),
    );
    res.status(500).json({error: 'Failed to forward metrics'});
  }
}
