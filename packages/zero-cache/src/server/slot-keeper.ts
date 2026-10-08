import {resolve} from 'node:path';
import type {Readable} from 'node:stream';
import {fileURLToPath} from 'node:url';
import {PG_ADMIN_SHUTDOWN} from '@drdgvhbh/postgres-error-codes';
import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {defu} from 'defu';
import postgres, {type Options, type PostgresType} from 'postgres';
import {sleep} from '../../../shared/src/sleep.ts';
import {
  DEFAULT_RETRIES_IF_REPLICATION_SLOT_ACTIVE,
  makeAck,
  startReplicationStream,
  type PgConnectionConfig,
} from '../services/change-source/pg/logical-replication/stream.ts';
import {toBigInt} from '../services/change-source/pg/lsn.ts';
import {exitAfter} from '../services/life-cycle.ts';
import {inactivityTimeoutSocket, isPostgresError} from '../types/pg.ts';
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
  ackIntervalMs?: number | undefined;
};

export function parseLsn(lsn: string): bigint {
  return lsn.includes('/') ? toBigInt(lsn) : BigInt(lsn);
}

export function waitForStartConfig(parent: Worker): Promise<SlotKeeperConfig> {
  return new Promise<SlotKeeperConfig>((resolve, reject) => {
    const cleanup = () => {
      parent.off('disconnect', onDisconnect);
      parent.off('close', onClose);
      if (process.send) {
        process.off('disconnect', onDisconnect);
      }
    };
    const onDisconnect = () => {
      cleanup();
      reject(new Error('Parent disconnected before sending start config'));
    };
    const onClose = () => {
      cleanup();
      reject(new Error('Parent closed before sending start config'));
    };
    parent.once('disconnect', onDisconnect);
    parent.once('close', onClose);
    if (process.send) {
      process.once('disconnect', onDisconnect);
    }
    parent.onceMessageType<['start', SlotKeeperConfig]>('start', cfg => {
      cleanup();
      resolve(cfg);
    });
    parent.send(['init', {}]);
  });
}

export type SlotKeeperDependencies = {
  createSession?: (
    config: SlotKeeperConfig,
    lc: LogContext,
  ) => postgres.Sql | {end: (opts?: {timeout?: number}) => Promise<void>};
  startStream?: (
    lc: LogContext,
    session: postgres.Sql,
    slot: string,
    publications: string[],
    startLsn: bigint,
    maxAttempts?: number,
  ) => Promise<[Readable, NodeJS.WritableStream]>;
  sleepFn?: (ms: number) => Promise<void>;
  logContext?: LogContext;
};

let lc = new LogContext('info', {}, consoleLogSink);

export default async function runWorker(
  parent: Worker,
  _env: NodeJS.ProcessEnv,
  depsOrArg?: string | SlotKeeperDependencies,
  ..._argv: string[]
): Promise<void> {
  const deps: SlotKeeperDependencies =
    typeof depsOrArg === 'object' && depsOrArg !== null ? depsOrArg : {};
  const createSession =
    deps.createSession ??
    ((cfg: SlotKeeperConfig, log: LogContext) =>
      postgres(
        defu(
          {
            max: 1,
            fetch_types: false,
            idle_timeout: null,
            max_lifetime: null as unknown as number,
            connection: {
              application_name: `slot-keeper-${cfg.slot}`,
              replication: 'database',
            },
            socket: inactivityTimeoutSocket(log, 0, 60_000),
          },
          cfg.db as Options<Record<string, PostgresType>>,
        ),
      ));
  const startStream = deps.startStream ?? startReplicationStream;
  const sleepFn = deps.sleepFn ?? sleep;

  const config = await waitForStartConfig(parent);

  lc = (deps.logContext ?? lc).withContext('slot', config.slot);
  lc.info?.(`starting slot-keeper for ${config.slot}`);

  let stopped = false;
  let stopResolve: (() => void) | null = null;
  let currentReadable: Readable | null = null;
  let currentSession:
    | postgres.Sql
    | {end: (opts?: {timeout?: number}) => Promise<void>}
    | null = null;
  const lsn = parseLsn(config.lsn);
  const ackIntervalMs = config.ackIntervalMs ?? 5_000;

  const closeCurrent = async () => {
    if (currentReadable) {
      try {
        currentReadable.destroy();
      } catch {}
      currentReadable = null;
    }
    if (currentSession) {
      try {
        await currentSession.end({timeout: 0});
        lc.info?.(`ended slot-keeper replication session for ${config.slot}`);
      } catch (e) {
        lc.warn?.(`error ending slot-keeper session`, e);
      }
      currentSession = null;
    }
  };

  const stop = async () => {
    if (stopped) {
      return;
    }
    stopped = true;
    lc.info?.(`stopping slot-keeper for ${config.slot}`);
    stopResolve?.();
    await closeCurrent();
  };

  parent.onceMessageType('stop', () => void stop());
  parent.on('SIGTERM', () => void stop());
  parent.on('SIGINT', () => void stop());
  parent.once('disconnect', () => void stop());
  parent.once('close', () => void stop());
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
      const session = createSession(config, lc);
      currentSession = session;

      const [readable, writeable] = await startStream(
        lc,
        session as postgres.Sql,
        config.slot,
        [config.dummyPublication],
        lsn,
        DEFAULT_RETRIES_IF_REPLICATION_SLOT_ACTIVE + 1,
      );
      currentReadable = readable;
      // Discard the streamed messages. Otherwise the paused socket backs up the
      // wal_sender (e.g. with keepalives and logical messages) for the duration
      // of the initial sync / restore, and a wal_sender blocked on writing to the
      // client does not exit (and release the slot) when it is terminated by the
      // takeover; it instead blocks on sending the termination error to the
      // client, until signaled again.
      readable.resume();

      if (stopped) {
        await closeCurrent();
        break;
      }

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

      // Wait until stream closes or errors, or worker is stopped
      await new Promise<void>((resolve, reject) => {
        const onStop = () => {
          cleanup();
          resolve();
        };
        const onClose = () => {
          cleanup();
          resolve();
        };
        const onError = (err: unknown) => {
          cleanup();
          reject(err);
        };
        function cleanup() {
          clearInterval(ackInterval);
          readable.off('close', onClose);
          readable.off('error', onError);
        }
        stopResolve = onStop;
        readable.once('close', onClose);
        readable.once('error', onError);
      });

      await closeCurrent();

      if (stopped) {
        break;
      }
    } catch (e) {
      await closeCurrent();

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

      await Promise.race([
        sleepFn(delayMs),
        new Promise<void>(resolve => {
          stopResolve = resolve;
        }),
      ]);
      delayMs = Math.min(delayMs * 2, 10_000);
    }
  }

  lc.info?.(`slot-keeper finished for ${config.slot}`);
}

// fork()
const isMain =
  typeof process.argv[1] === 'string' &&
  fileURLToPath(import.meta.url) === resolve(process.argv[1]);

const parent = parentWorker;
if (isMain && parent !== null && !singleProcessMode()) {
  void exitAfter(
    () => lc,
    () => runWorker(parent, process.env, ...process.argv.slice(2)),
  );
}
