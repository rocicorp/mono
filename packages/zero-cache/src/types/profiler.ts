/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
