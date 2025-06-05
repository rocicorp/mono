import type {LogContext} from '@rocicorp/logger';
import {writeFile} from 'node:fs/promises';
import {Session} from 'node:inspector/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';

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

  async stopAndDispose(lc: LogContext, filename: string) {
    const {profile} = await this.#session.post('Profiler.stop');
    const path = join(tmpdir(), `${filename}.cpuprofile`);
    await writeFile(path, JSON.stringify(profile));
    lc.info?.(`wrote cpu profile to ${path}`);
    this.#session.disconnect();
  }
}
