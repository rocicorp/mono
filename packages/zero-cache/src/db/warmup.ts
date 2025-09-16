/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import type {LogContext} from '@rocicorp/logger';
import type {PostgresDB} from '../types/pg.ts';

export async function warmupConnections(
  lc: LogContext,
  db: PostgresDB,
  name: string,
) {
  const {max, host} = db.options;
  await Promise.allSettled(
    Array.from({length: max}, () => db`SELECT 1`.simple().execute()),
  );
  const start = performance.now();
  const pingTimes = await Promise.all(
    Array.from({length: Math.min(max, 5)}, () =>
      db`SELECT 2`.simple().then(
        () => performance.now() - start,
        () => performance.now() - start,
      ),
    ),
  );
  const average = pingTimes.reduce((l, r) => l + r, 0) / pingTimes.length;
  const log = average >= 10 ? 'warn' : 'info';
  lc[log]?.(`average ping to ${name} db@${host}: ${average.toFixed(2)} ms`);
  if (log === 'warn') {
    lc.warn?.(`ideal db ping time is < 5 ms`);
  }
}
