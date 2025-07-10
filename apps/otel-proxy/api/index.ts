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
 * OTEL proxy handler that forwards metrics to Grafana OTLP endpoint.
 * Validates requests and forwards with proper authentication.
 */
export default async function handler(
  req: VercelRequest,
  res: VercelResponse,
): Promise<void> {
  // eslint-disable-next-line no-console
  console.log(`OTEL Proxy: ${req.method} request received`);

  if (req.method !== 'POST') {
    // eslint-disable-next-line no-console
    console.log('Rejecting non-POST request');
    res.status(405).json({error: 'Method not allowed'});
    return;
  }

  const {ROCICORP_TELEMETRY_TOKEN, GRAFANA_OTLP_ENDPOINT} = process.env;
  const endpoint =
    GRAFANA_OTLP_ENDPOINT ||
    'https://otlp-gateway-prod-us-east-2.grafana.net/otlp/v1/metrics';

  // eslint-disable-next-line no-console
  console.log(
    `Token configured: ${!!ROCICORP_TELEMETRY_TOKEN}, Endpoint: ${endpoint}`,
  );
  // eslint-disable-next-line no-console
  console.log(
    `Incoming Content-Type: ${req.headers?.['content-type'] || 'not set'}`,
  );

  if (!ROCICORP_TELEMETRY_TOKEN) {
    // eslint-disable-next-line no-console
    console.error('ROCICORP_TELEMETRY_TOKEN not configured');
    res.status(500).json({error: 'ROCICORP_TELEMETRY_TOKEN not configured'});
    return;
  }

  try {
    // Validate request body exists
    assert(req.body, 'Request body is required for metrics forwarding');

    // Properly handle the request body based on content type
    const contentType =
      req.headers?.['content-type'] || 'application/x-protobuf';
    let bodyToSend: string | Buffer;

    if (contentType.includes('application/json')) {
      // For JSON, ensure we send properly stringified JSON
      bodyToSend =
        typeof req.body === 'string' ? req.body : JSON.stringify(req.body);
      // eslint-disable-next-line no-console
      console.log(`JSON body prepared, length: ${bodyToSend.length}`);
    } else {
      // For protobuf or other binary formats, send as-is
      bodyToSend = req.body;
      // eslint-disable-next-line no-console
      console.log(
        `Binary body prepared, length: ${req.body ? String(req.body).length : 0}`,
      );
    }

    // eslint-disable-next-line no-console
    console.log('Forwarding to Grafana...');

    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': contentType,
        'authorization': `Bearer ${ROCICORP_TELEMETRY_TOKEN}`,
      },
      body: bodyToSend,
    });

    // eslint-disable-next-line no-console
    console.log(`Grafana response: ${response.status}`);

    // Forward the response status and body from Grafana
    res.status(response.status);

    if (response.headers.get('content-type')?.includes('application/json')) {
      const jsonResponse = await response.json();
      // eslint-disable-next-line no-console
      console.log('Grafana JSON response:', JSON.stringify(jsonResponse));
      res.json(jsonResponse);
      return;
    }

    const textResponse = await response.text();
    // eslint-disable-next-line no-console
    console.log('Grafana text response:', textResponse);
    res.send(textResponse);
  } catch (error) {
    // eslint-disable-next-line no-console
    console.error(
      'Error forwarding metrics:',
      error instanceof Error ? error.message : String(error),
    );
    res.status(500).json({error: 'Failed to forward metrics'});
  }
}
