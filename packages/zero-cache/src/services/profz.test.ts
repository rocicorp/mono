import fastify, {type FastifyInstance} from 'fastify';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {CpuProfiler} from '../types/profiler.ts';
import {handleProfrmzRequest, handleProfzRequest} from './profz.ts';

describe('profz', () => {
  const lc = createSilentLogContext();
  const config = {
    adminPassword: 'secret',
    changeStreamer: {},
  } as unknown as NormalizedZeroConfig;

  const authHeader = {
    authorization: `Basic ${Buffer.from('user:secret').toString('base64')}`,
  };

  let app: FastifyInstance;
  let profileSpy: ReturnType<typeof vi.spyOn>;

  const mockProfile = {
    nodes: [{id: 1, callFrame: {functionName: 'root'}}],
    samples: [1],
    timeDeltas: [1000],
    startTime: 0,
    endTime: 1000,
  };

  beforeEach(async () => {
    profileSpy = vi
      .spyOn(CpuProfiler, 'profile')
      .mockResolvedValue(mockProfile);

    app = fastify();
    app.get('/profz', (req, res) => handleProfzRequest(lc, config, req, res));
    app.get('/profrmz', (req, res) =>
      handleProfrmzRequest(lc, config, req, res),
    );
    await app.ready();
  });

  afterEach(async () => {
    profileSpy.mockRestore();
    await app.close();
  });

  test('requires auth when adminPassword is set', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/profz',
    });

    expect(res.statusCode).toBe(401);
  });

  test('returns multi-process profile bundle by default', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/profz?duration=1',
      headers: authHeader,
    });

    expect(res.statusCode).toBe(200);
    expect(res.headers['content-type']).toContain('application/json');
    expect(res.headers['content-disposition']).toContain('zero-profile-bundle');

    const body = JSON.parse(res.body) as Record<string, typeof mockProfile>;
    expect(body).toHaveProperty('dispatcher');
    expect(body.dispatcher).toEqual(mockProfile);
    expect(profileSpy).toHaveBeenCalledWith(1000);
  });

  test('returns single profile when specific worker is requested', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/profz?duration=2&worker=dispatcher',
      headers: authHeader,
    });

    expect(res.statusCode).toBe(200);
    expect(res.headers['content-type']).toContain('application/json');
    expect(res.headers['content-disposition']).toContain(
      'dispatcher.cpuprofile',
    );

    const body = JSON.parse(res.body) as typeof mockProfile;
    expect(body).toEqual(mockProfile);
    expect(profileSpy).toHaveBeenCalledWith(2000);
  });

  test('clamps duration between 1 and 60 seconds', async () => {
    await app.inject({
      method: 'GET',
      url: '/profz?duration=100',
      headers: authHeader,
    });
    expect(profileSpy).toHaveBeenLastCalledWith(60_000);

    await app.inject({
      method: 'GET',
      url: '/profz?duration=0',
      headers: authHeader,
    });
    expect(profileSpy).toHaveBeenLastCalledWith(1_000);
  });

  test('profrmz in single-node mode profiles change-streamer', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/profrmz?duration=1',
      headers: authHeader,
    });

    // In local mode without changeStreamer.uri, it profiles change-streamer directly
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body) as typeof mockProfile;
    expect(body).toEqual(mockProfile);
  });

  test('profrmz in distributed mode proxies to changeStreamer.uri', async () => {
    const distributedConfig = {
      adminPassword: 'secret',
      changeStreamer: {
        uri: 'http://127.0.0.1:4849',
      },
    } as unknown as NormalizedZeroConfig;

    const distApp = fastify();
    distApp.get('/profrmz', (req, res) =>
      handleProfrmzRequest(lc, distributedConfig, req, res),
    );
    await distApp.ready();

    // Mock global fetch
    const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({'change-streamer': mockProfile}), {
        status: 200,
        headers: {
          'content-type': 'application/json',
          'content-disposition': 'attachment; filename="rm-bundle.json"',
        },
      }),
    );

    const res = await distApp.inject({
      method: 'GET',
      url: '/profrmz?duration=5',
      headers: authHeader,
    });

    expect(res.statusCode).toBe(200);
    expect(fetchSpy).toHaveBeenCalledWith(
      'http://127.0.0.1:4849/profz?duration=5',
      expect.objectContaining({
        method: 'GET',
      }),
    );
    const body = JSON.parse(res.body) as Record<string, typeof mockProfile>;
    expect(body).toHaveProperty('change-streamer');

    fetchSpy.mockRestore();
    await distApp.close();
  });
});
