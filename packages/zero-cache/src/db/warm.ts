import type {LogContext} from '@rocicorp/logger';
import type {PostgresDB} from '../types/pg.ts';

export async function warm(lc: LogContext, db: PostgresDB, name: string) {
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
