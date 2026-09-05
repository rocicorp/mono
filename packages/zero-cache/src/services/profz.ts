import {randomUUID} from 'node:crypto';
import type {LogContext} from '@rocicorp/logger';
import auth from 'basic-auth';
import type {FastifyReply, FastifyRequest} from 'fastify';
import {sleep} from '../../../shared/src/sleep.ts';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {isAdminPasswordValid} from '../config/zero-config.ts';
import {
  singleProcessMode,
  type ProfileResponse,
  type ProfileResponseMessage,
  type Worker,
} from '../types/processes.ts';
import {CpuProfiler} from '../types/profiler.ts';
import {URLParams} from '../types/url-params.ts';

export async function handleProfzRequest(
  lc: LogContext,
  config: Pick<NormalizedZeroConfig, 'adminPassword'>,
  req: FastifyRequest,
  res: FastifyReply,
  getWorker?: (() => Promise<Worker>) | undefined,
  forcedWorker?: string | undefined,
  localProcessName: string = 'dispatcher',
): Promise<void> {
  const credentials = auth(req);
  if (!isAdminPasswordValid(lc, config, credentials?.pass)) {
    void res
      .code(401)
      .header('WWW-Authenticate', 'Basic realm="Profz Protected Area"')
      .send('Unauthorized');
    return;
  }

  const params = new URLParams(new URL(req.url, 'http://localhost'));
  const rawDuration =
    params.getInteger('duration', false) ??
    params.getInteger('seconds', false) ??
    5;
  const durationSec = Math.max(1, Math.min(60, rawDuration));
  const durationMs = durationSec * 1000;
  const targetWorker = forcedWorker ?? params.get('worker', false) ?? undefined;
  const targetWorkerIndex = params.getInteger('index', false) ?? undefined;
  const id = randomUUID();

  const responses = new Map<string, unknown>();

  // Profile dispatcher / local process if requested, when profiling all processes,
  // or when no worker dispatcher is available (single process / standalone mode)
  const shouldProfileLocal =
    singleProcessMode() ||
    getWorker === undefined ||
    targetWorker === undefined ||
    targetWorker === localProcessName;

  const localProfilePromise = shouldProfileLocal
    ? CpuProfiler.profile(durationMs).catch(err => {
        lc.warn?.('Failed to capture local CPU profile:', err);
        return null;
      })
    : Promise.resolve(null);

  if (!singleProcessMode() && getWorker !== undefined) {
    try {
      const worker = await getWorker();
      const onMessage = (msg: ProfileResponse) => {
        if (msg.id === id && msg.profile) {
          responses.set(msg.name, msg.profile);
        } else if (msg.id === id && msg.error) {
          lc.warn?.(`Worker ${msg.name} profile error: ${msg.error}`);
        }
      };

      worker.onMessageType<ProfileResponseMessage>(
        'profileResponse',
        onMessage,
      );
      worker.send([
        'profile',
        {
          id,
          durationMs,
          worker: targetWorker === localProcessName ? '__none__' : targetWorker,
          workerIndex: targetWorkerIndex,
        },
      ]);

      // Wait for duration plus grace period for IPC transfer
      await sleep(durationMs + 500);
    } catch (err) {
      lc.warn?.('Failed to dispatch profile request to child workers:', err);
    }
  }

  const localProfile = await localProfilePromise;
  if (localProfile) {
    const localName =
      targetWorker ?? (singleProcessMode() ? 'zero-cache' : localProcessName);
    responses.set(localName, localProfile);
  }

  // If a single specific worker was requested, return that profile directly
  if (targetWorker !== undefined && targetWorker !== 'all') {
    let matchedProfile = responses.get(targetWorker);
    if (!matchedProfile) {
      // Try matching by prefix or substring (e.g. 'syncer' matches 'syncer-0')
      for (const [name, prof] of responses.entries()) {
        if (name === targetWorker || name.startsWith(`${targetWorker}-`)) {
          matchedProfile = prof;
          break;
        }
      }
    }

    if (matchedProfile) {
      void res
        .header('Content-Type', 'application/json')
        .header(
          'Content-Disposition',
          `attachment; filename="${targetWorker}.cpuprofile"`,
        )
        .send(matchedProfile);
      return;
    }

    void res.code(404).send({
      error: `No profile captured for worker "${targetWorker}". Available: ${[...responses.keys()].join(', ')}`,
    });
    return;
  }

  // Default (Option A): Return multi-process profile bundle as a JSON map
  const bundle: Record<string, unknown> = {};
  for (const [name, prof] of responses.entries()) {
    bundle[name] = prof;
  }

  void res
    .header('Content-Type', 'application/json')
    .header(
      'Content-Disposition',
      `attachment; filename="zero-profile-bundle-${Date.now()}.json"`,
    )
    .send(bundle);
}

export async function handleProfrmzRequest(
  lc: LogContext,
  config: NormalizedZeroConfig,
  req: FastifyRequest,
  res: FastifyReply,
  getWorker?: (() => Promise<Worker>) | undefined,
): Promise<void> {
  const credentials = auth(req);
  if (!isAdminPasswordValid(lc, config, credentials?.pass)) {
    void res
      .code(401)
      .header('WWW-Authenticate', 'Basic realm="Profrmz Protected Area"')
      .send('Unauthorized');
    return;
  }

  // In distributed mode, proxy to the upstream Replication Manager
  if (config.changeStreamer.uri) {
    const upstreamURL = new URL('/profz', config.changeStreamer.uri);
    const incomingURL = new URL(req.url, 'http://localhost');
    for (const [key, value] of incomingURL.searchParams.entries()) {
      upstreamURL.searchParams.set(key, value);
    }

    try {
      const headers: Record<string, string> = {};
      if (req.headers.authorization) {
        headers.authorization = req.headers.authorization;
      }
      const response = await fetch(upstreamURL.toString(), {
        method: 'GET',
        headers,
      });

      res.code(response.status);
      const contentType = response.headers.get('content-type');
      if (contentType) {
        res.header('content-type', contentType);
      }
      const contentDisposition = response.headers.get('content-disposition');
      if (contentDisposition) {
        res.header('content-disposition', contentDisposition);
      }

      const body = await response.text();
      return res.send(body);
    } catch (err) {
      lc.error?.(`Failed to proxy /profrmz to RM at ${upstreamURL}:`, err);
      return res.code(502).send({
        error: `Failed to proxy profiling request to RM at ${upstreamURL}: ${String(err)}`,
      });
    }
  }

  // In single-node mode, profile local change-streamer / RM process
  return handleProfzRequest(lc, config, req, res, getWorker, 'change-streamer');
}
