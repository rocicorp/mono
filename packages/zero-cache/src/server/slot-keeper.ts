import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {PG_ADMIN_SHUTDOWN} from '@drdgvhbh/postgres-error-codes';
import {defu} from 'defu';
import postgres, {type Options, type PostgresType} from 'postgres';
import {must} from '../../../shared/src/must.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import {exitAfter} from '../services/life-cycle.ts';
import {
  makeAck,
  startReplicationStream,
  type PgConnectionConfig,
} from '../services/change-source/pg/logical-replication/stream.ts';
import {isPostgresError} from '../types/pg.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';

export type SlotKeeperConfig = {
  db: PgConnectionConfig;
  slot: string;
  dummyPublication: string;
  lsn: string;
  ackIntervalMs?: number;
};

let lc = new LogContext('info', {}, consoleLogSink);

export default async function runWorker(
  parent: Worker,
  _env: NodeJS.ProcessEnv,
  ...argv: string[]
): Promise<void> {
  const config: SlotKeeperConfig = JSON.parse(argv[0]);
  lc = lc.withContext('slot', config.slot);
  lc.info?.(`starting slot-keeper for ${config.slot}`);

  let stopped = false;
  let currentSession: postgres.Sql | null = null;
  const lsn = BigInt(config.lsn);
  const ackIntervalMs = config.ackIntervalMs ?? 5_000;

  const stop = async () => {
    if (stopped) {
      return;
    }
    stopped = true;
    lc.info?.(`stopping slot-keeper for ${config.slot}`);
    if (currentSession) {
      try {
        await currentSession.end({timeout: 2});
        lc.info?.(`ended slot-keeper replication session for ${config.slot}`);
      } catch (e) {
        lc.warn?.(`error ending slot-keeper session`, e);
      }
      currentSession = null;
    }
  };

  parent.onceMessageType('stop', () => void stop());
  parent.on('SIGTERM', () => void stop());
  parent.on('SIGINT', () => void stop());
  process.once('SIGTERM', () => void stop());
  process.once('SIGINT', () => void stop());
  if (process.send) {
    process.once('disconnect', () => void stop());
  }

  let firstAttempt = true;
  let delayMs = 1000;

  while (!stopped) {
    try {
      lc.info?.(`connecting replication session for slot ${config.slot}`);
      const session = postgres(
        defu(
          {
            max: 1,
            fetch_types: false,
            idle_timeout: null,
            max_lifetime: null as unknown as number,
            connection: {
              application_name: `slot-keeper-${config.slot}`,
              replication: 'database',
            },
          },
          config.db as Options<Record<string, PostgresType>>,
        ),
      );
      currentSession = session;

      const [readable, writeable] = await startReplicationStream(
        lc,
        session,
        config.slot,
        [config.dummyPublication],
        lsn,
        1,
      );

      lc.info?.(`slot-keeper active for ${config.slot} at lsn ${config.lsn}`);
      if (firstAttempt) {
        firstAttempt = false;
        parent.send(['ready', {ready: true}]);
      }

      // Reset backoff delay on successful connection
      delayMs = 1000;

      const ackInterval = setInterval(() => {
        if (stopped) {
          clearInterval(ackInterval);
          return;
        }
        try {
          writeable.write(makeAck(lsn));
        } catch (e) {
          lc.warn?.(`error writing ACK to replication stream`, e);
        }
      }, ackIntervalMs);

      // Wait until stream closes or errors
      await new Promise<void>((resolve, reject) => {
        readable.once('close', () => {
          clearInterval(ackInterval);
          resolve();
        });
        readable.once('error', err => {
          clearInterval(ackInterval);
          reject(err);
        });
      });

      try {
        await session.end({timeout: 2});
      } catch {}
      currentSession = null;

      if (stopped) {
        break;
      }
    } catch (e) {
      if (currentSession) {
        try {
          await currentSession.end({timeout: 2});
        } catch {}
        currentSession = null;
      }

      if (stopped) {
        break;
      }

      if (
        isPostgresError(e, PG_ADMIN_SHUTDOWN) ||
        (e instanceof Error &&
          (e.message.includes('57P01') ||
            e.message.includes(
              'terminating connection due to administrator command',
            )))
      ) {
        lc.info?.(
          `slot ${config.slot} was taken over (admin shutdown / pg_terminate_backend)`,
        );
        break;
      }

      lc.warn?.(
        `slot-keeper stream error for ${config.slot}, will reconnect in ${delayMs}ms`,
        e,
      );

      if (firstAttempt) {
        throw e;
      }

      await sleep(delayMs);
      delayMs = Math.min(delayMs * 2, 10_000);
    }
  }

  lc.info?.(`slot-keeper finished for ${config.slot}`);
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(
    () => lc,
    () => runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
