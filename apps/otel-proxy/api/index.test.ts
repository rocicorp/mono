import {describe, it, expect, vi, beforeEach} from 'vitest';
import type {VercelRequest, VercelResponse} from '@vercel/node';

import handler from './index';

// Mock fetch
const mockFetch = vi.fn();
global.fetch = mockFetch;

// Sample OTEL metrics data
const validOtelData = {
  resourceMetrics: [
    {
      resource: {
        attributes: [
          {
            key: 'service.name',
            value: {stringValue: 'test-service'},
          },
        ],
      },
      scopeMetrics: [
        {
          scope: {
            name: 'test-scope',
          },
          metrics: [
            {
              name: 'test_metric',
              unit: 'count',
              sum: {
                dataPoints: [
                  {
                    timeUnixNano: '1234567890000000000',
                    value: 42,
                  },
                ],
              },
            },
          ],
        },
      ],
    },
  ],
};

describe('OTEL Proxy Handler', () => {
  let req: Partial<VercelRequest>;
  let res: Partial<VercelResponse>;

  beforeEach(() => {
    vi.clearAllMocks();

    // Reset env vars to defaults
    vi.stubEnv('ROCICORP_TELEMETRY_TOKEN', 'test-token');
    vi.stubEnv(
      'GRAFANA_OTLP_ENDPOINT',
      'https://test-grafana.com/otlp/v1/metrics',
    );

    req = {
      method: 'POST',
      body: validOtelData,
      headers: {
        'content-type': 'application/json',
      },
    };

    res = {
      status: vi.fn().mockReturnThis(),
      json: vi.fn().mockReturnThis(),
      send: vi.fn().mockReturnThis(),
    };
  });

  it('should forward POST requests to Grafana successfully', async () => {
    const mockResponse = {
      status: 200,
      headers: {
        get: vi.fn().mockReturnValue('application/json'),
      },
      json: vi.fn().mockResolvedValue({success: true}),
    };
    mockFetch.mockResolvedValue(mockResponse);

    await handler(req as VercelRequest, res as VercelResponse);

    expect(mockFetch).toHaveBeenCalledWith(
      'https://test-grafana.com/otlp/v1/metrics',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'authorization': 'Bearer test-token',
        },
        body: JSON.stringify(validOtelData),
      },
    );

    expect(res.status).toHaveBeenCalledWith(200);
    expect(res.json).toHaveBeenCalledWith({success: true});
  });

  it('should handle text responses from Grafana', async () => {
    const mockResponse = {
      status: 200,
      headers: {
        get: vi.fn().mockReturnValue('text/plain'),
      },
      text: vi.fn().mockResolvedValue('OK'),
    };
    mockFetch.mockResolvedValue(mockResponse);

    await handler(req as VercelRequest, res as VercelResponse);

    expect(res.status).toHaveBeenCalledWith(200);
    expect(res.send).toHaveBeenCalledWith('OK');
  });

  it('should reject non-POST requests', async () => {
    req.method = 'GET';

    await handler(req as VercelRequest, res as VercelResponse);

    expect(res.status).toHaveBeenCalledWith(405);
    expect(res.json).toHaveBeenCalledWith({error: 'Method not allowed'});
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('should reject non-JSON content type', async () => {
    req.headers = {'content-type': 'application/x-protobuf'};

    await handler(req as VercelRequest, res as VercelResponse);

    expect(res.status).toHaveBeenCalledWith(400);
    expect(res.json).toHaveBeenCalledWith({error: 'Invalid request type'});
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('should reject requests with missing content type', async () => {
    req.headers = {};

    await handler(req as VercelRequest, res as VercelResponse);

    expect(res.status).toHaveBeenCalledWith(400);
    expect(res.json).toHaveBeenCalledWith({error: 'Invalid request type'});
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('should reject requests without OTEL data', async () => {
    req.body = {some: 'random', data: 'without otel fields'};

    await handler(req as VercelRequest, res as VercelResponse);

    expect(res.status).toHaveBeenCalledWith(400);
    expect(res.json).toHaveBeenCalledWith({error: 'Invalid request body'});
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('should accept requests with resource field', async () => {
    req.body = {
      resource: {
        attributes: [{key: 'service.name', value: {stringValue: 'test'}}],
      },
    };

    const mockResponse = {
      status: 200,
      headers: {
        get: vi.fn().mockReturnValue('application/json'),
      },
      json: vi.fn().mockResolvedValue({success: true}),
    };
    mockFetch.mockResolvedValue(mockResponse);

    await handler(req as VercelRequest, res as VercelResponse);

    expect(res.status).toHaveBeenCalledWith(200);
    expect(mockFetch).toHaveBeenCalled();
  });

  it('should handle missing ROCICORP_TELEMETRY_TOKEN', async () => {
    vi.stubEnv('ROCICORP_TELEMETRY_TOKEN', '');

    await handler(req as VercelRequest, res as VercelResponse);

    expect(res.status).toHaveBeenCalledWith(500);
    expect(res.json).toHaveBeenCalledWith({
      error: 'Telemetry token not configured',
    });
    expect(mockFetch).not.toHaveBeenCalled();
  });

  it('should handle fetch errors', async () => {
    mockFetch.mockRejectedValue(new Error('Network error'));

    await handler(req as VercelRequest, res as VercelResponse);

    expect(res.status).toHaveBeenCalledWith(500);
    expect(res.json).toHaveBeenCalledWith({error: 'Failed to forward metrics'});
  });

  it('should forward non-200 status codes from Grafana', async () => {
    const mockResponse = {
      status: 400,
      headers: {
        get: vi.fn().mockReturnValue('application/json'),
      },
      json: vi.fn().mockResolvedValue({error: 'Bad Request'}),
    };
    mockFetch.mockResolvedValue(mockResponse);

    await handler(req as VercelRequest, res as VercelResponse);

    expect(res.status).toHaveBeenCalledWith(400);
    expect(res.json).toHaveBeenCalledWith({error: 'Bad Request'});
  });

  it('should use default endpoint when GRAFANA_OTLP_ENDPOINT is not set', async () => {
    vi.stubEnv('GRAFANA_OTLP_ENDPOINT', '');

    const mockResponse = {
      status: 200,
      headers: {
        get: vi.fn().mockReturnValue('application/json'),
      },
      json: vi.fn().mockResolvedValue({success: true}),
    };
    mockFetch.mockResolvedValue(mockResponse);

    await handler(req as VercelRequest, res as VercelResponse);

    expect(mockFetch).toHaveBeenCalledWith(
      'https://otlp-gateway-prod-us-east-2.grafana.net/otlp/v1/metrics',
      expect.any(Object),
    );
  });

  it('should validate request body exists', async () => {
    req.body = undefined;

    await handler(req as VercelRequest, res as VercelResponse);

    expect(res.status).toHaveBeenCalledWith(500);
    expect(res.json).toHaveBeenCalledWith({error: 'Failed to forward metrics'});
    expect(mockFetch).not.toHaveBeenCalled();
  });
});
