import {writeFile} from 'node:fs/promises';
import {Session} from 'node:inspector/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import type {LogContext} from '@rocicorp/logger';
import type {ProfileMessage, Worker} from './processes.ts';

/**
 * Convenience wrapper around a `node:inspector` {@link Session} for
 * optionally taking cpu profiles.
 */
export class CpuProfiler {
  static async connect() {
    const session = new Session();
    session.connect();
    await session.post('Profiler.enable');
    return new CpuProfiler(session);
  }

  readonly #session;

  private constructor(session: Session) {
    this.#session = session;
  }

  async start() {
    await this.#session.post('Profiler.start');
  }

  /**
   * Captures a CPU profile for the specified duration (in milliseconds)
   * and returns the V8 profile object.
   */
  static async profile(durationMs: number): Promise<unknown> {
    const profiler = await CpuProfiler.connect();
    await profiler.start();
    await new Promise(resolve => setTimeout(resolve, durationMs));
    return await profiler.stop();
  }

  async stop(): Promise<unknown> {
    const {profile} = await this.#session.post('Profiler.stop');
    await this.#session.post('Profiler.disable');
    this.#session.disconnect();
    return profile;
  }

  async stopAndDispose(lc: LogContext, filename: string) {
    const {profile} = await this.#session.post('Profiler.stop');
    const path = join(tmpdir(), `${filename}.cpuprofile`);
    await writeFile(path, JSON.stringify(profile));
    lc.info?.(`wrote cpu profile to ${path}`);
    this.#session.disconnect();
  }
}

const WORKER_INDEX_SUFFIX = /-\d+$/;

/**
 * Registers an IPC handler on `parent` to capture a CPU profile when
 * requested, sending back the resulting V8 profile.
 */
export function installProfileHandler(
  parent: Worker | null,
  workerName: string,
  workerIndex?: number | undefined,
): void {
  if (!parent) {
    return;
  }
  parent.onMessageType<ProfileMessage>('profile', async req => {
    if (req.worker !== undefined) {
      const baseName = workerName.replace(WORKER_INDEX_SUFFIX, '');
      const matchesName = req.worker === workerName || req.worker === baseName;
      if (!matchesName) {
        return;
      }
      if (
        req.workerIndex !== undefined &&
        workerIndex !== undefined &&
        req.workerIndex !== workerIndex
      ) {
        return;
      }
    }
    try {
      const profile = await CpuProfiler.profile(req.durationMs);
      parent.send(['profileResponse', {id: req.id, name: workerName, profile}]);
    } catch (err) {
      parent.send([
        'profileResponse',
        {id: req.id, name: workerName, error: String(err)},
      ]);
    }
  });
}
