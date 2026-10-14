import type {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';

/**
 * Whether the imported postgres.js has Zero's patch
 * (`packages/zero/patches/postgres@3.4.7.patch`), which marks itself with
 * `Postgres.zeroPatch`. pnpm applies the patch in this repo and in the Docker
 * image, but not when `@rocicorp/zero` is installed from npm.
 *
 * The patch includes the `CopyDone` fix for
 * https://github.com/porsager/postgres/issues/499
 * (https://github.com/porsager/postgres/pull/1016). Without it, a connection
 * can stop responding after a COPY, on any Node version: if the last
 * `CopyData` fills the stream's buffer, the socket is paused, and it is never
 * resumed once the stream has ended. Node version and OS only change how often
 * this happens. Initial sync and backfill only wait for their COPY connections
 * to close when this returns `true`.
 *
 * A later version of the patch can raise the number to mark new fixes.
 */
export function hasPostgresPatch(): boolean {
  return (postgres as {zeroPatch?: number}).zeroPatch !== undefined;
}

export function warnIfPostgresUnpatched(lc: LogContext): void {
  if (!hasPostgresPatch()) {
    lc.warn?.(
      `postgres.js does not have Zero's patch for a bug that can leave a ` +
        `Postgres connection unresponsive after a COPY, on any Node version ` +
        `(https://github.com/porsager/postgres/pull/1016). Initial sync and ` +
        `backfill will not wait for their COPY connections to close, as in ` +
        `zero-cache 1.10. The rocicorp/zero Docker image includes the patch.`,
    );
  }
}
