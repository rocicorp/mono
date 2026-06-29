import type {LogContext} from '@rocicorp/logger';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import type {Message, Worker} from '../../types/processes.ts';
import {RunningState} from '../running-state.ts';
import type {Service} from '../service.ts';
import type {VfsBackupWatermark} from './vfs-watermark-reader.ts';

export type BackupWatermarkRequest = [
  'backupWatermarkRequest',
  {
    readonly requestID: string;
  },
];

export type BackupWatermarkResponse = [
  'backupWatermarkResponse',
  {
    readonly requestID: string;
    readonly result?: VfsBackupWatermark | undefined;
    readonly error?:
      | {
          readonly name: string;
          readonly message: string;
          readonly stack?: string | undefined;
        }
      | undefined;
  },
];

export interface BackupWatermarkSource {
  readWatermark(): VfsBackupWatermark;
  close?(): void;
}

export type BackupWatermarkSourceFactory = () => BackupWatermarkSource;

let nextRequestID = 0;

export class BackupWatermarkTimeoutError extends Error {}

export function requestVfsBackupWatermark(
  worker: Worker,
  timeoutMs: number,
  options: {
    readonly killOnTimeout?: boolean | undefined;
    readonly killSignal?: NodeJS.Signals | undefined;
  } = {},
): Promise<VfsBackupWatermark> {
  const requestID = String(++nextRequestID);
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      if (options.killOnTimeout) {
        worker.kill(options.killSignal ?? 'SIGKILL');
      }
      reject(
        new BackupWatermarkTimeoutError(
          `timed out waiting ${timeoutMs} ms for backup watermark response`,
        ),
      );
    }, timeoutMs);

    const onMessage = (data: unknown) => {
      const response = asBackupWatermarkResponse(data);
      if (response === null || response[1].requestID !== requestID) {
        return;
      }
      cleanup();
      const {result, error} = response[1];
      if (error !== undefined) {
        reject(new Error(error.message, {cause: error}));
        return;
      }
      if (result === undefined) {
        reject(new Error(`backup watermark response missing result`));
        return;
      }
      resolve(result);
    };

    const cleanup = () => {
      clearTimeout(timeout);
      worker.off('message', onMessage);
    };

    worker.on('message', onMessage);
    worker.send<BackupWatermarkRequest>(
      ['backupWatermarkRequest', {requestID}],
      undefined,
      (err: Error | null) => {
        if (err !== null) {
          cleanup();
          reject(err);
        }
      },
    );
  });
}

export class VfsBackupWatermarkWorkerService implements Service {
  readonly id = 'vfs-backup-watermark-worker';
  readonly #lc: LogContext;
  readonly #parent: Worker | null;
  readonly #sourceFactory: BackupWatermarkSourceFactory;
  #source: BackupWatermarkSource | undefined;
  readonly #state = new RunningState(this.id);
  readonly #pollIntervalMs: number;

  constructor(
    lc: LogContext,
    parent: Worker | null,
    source: BackupWatermarkSource | BackupWatermarkSourceFactory,
    pollIntervalMs: number,
  ) {
    this.#lc = lc.withContext('component', this.id);
    this.#parent = parent;
    this.#sourceFactory = typeof source === 'function' ? source : () => source;
    this.#pollIntervalMs = pollIntervalMs;
  }

  async run(): Promise<void> {
    try {
      if (this.#parent) {
        this.#parent.onMessageType<BackupWatermarkRequest>(
          'backupWatermarkRequest',
          msg => this.#handleRequest(msg),
        );
        this.#parent.send(['ready', {ready: true}]);
        await this.#state.stopped();
        return;
      }

      this.#lc.info?.(
        `polling backup watermark every ${this.#pollIntervalMs} ms`,
      );
      while (this.#state.shouldRun()) {
        try {
          this.#lc.info?.(
            `polled backup watermark`,
            this.#getSource().readWatermark(),
          );
        } catch (e) {
          this.#lc.warn?.(`error while polling backup watermark`, e);
        }
        await this.#state.sleep(this.#pollIntervalMs);
      }
    } finally {
      this.#source?.close?.();
    }
  }

  stop(): Promise<void> {
    this.#state.stop(this.#lc);
    return promiseVoid;
  }

  #handleRequest(msg: BackupWatermarkRequest[1]) {
    if (!this.#parent) {
      return;
    }
    try {
      this.#parent.send<BackupWatermarkResponse>([
        'backupWatermarkResponse',
        {
          requestID: msg.requestID,
          result: this.#getSource().readWatermark(),
        },
      ]);
    } catch (e) {
      this.#parent.send<BackupWatermarkResponse>([
        'backupWatermarkResponse',
        {
          requestID: msg.requestID,
          error: serializeError(e),
        },
      ]);
    }
  }

  #getSource(): BackupWatermarkSource {
    return (this.#source ??= this.#sourceFactory());
  }
}

function serializeError(e: unknown): BackupWatermarkResponse[1]['error'] {
  if (e instanceof Error) {
    return {
      name: e.name,
      message: e.message,
      stack: e.stack,
    };
  }
  return {
    name: 'Error',
    message: String(e),
  };
}

function asBackupWatermarkResponse(
  data: unknown,
): BackupWatermarkResponse | null {
  if (
    Array.isArray(data) &&
    data.length === 2 &&
    data[0] === 'backupWatermarkResponse'
  ) {
    return data as BackupWatermarkResponse;
  }
  return null;
}

export type BackupWatermarkMessage =
  | BackupWatermarkRequest
  | BackupWatermarkResponse
  | Message<unknown>;
