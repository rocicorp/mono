import type {LogContext} from '@rocicorp/logger';
import {
  isSQLiteChangeLogMaintenanceRequest,
  isSQLiteChangeLogMaintenanceResponse,
  validateSQLiteChangeLogMaintenanceRequest,
  validateSQLiteChangeLogMaintenanceResponse,
  type SQLiteChangeLogMaintenanceRequest,
  type SQLiteChangeLogMaintenanceResponse,
} from '../services/replicator/sqlite-change-log-maintenance.ts';
import {serializeError} from '../services/replicator/write-worker-client.ts';
import type {Worker} from '../types/processes.ts';

/**
 * Relays maintenance RPCs across the dispatcher without ever opening the
 * replica itself. The target must be the one canonical replicator selected by
 * the dispatcher (backup when present, otherwise primary serving).
 */
export class SQLiteChangeLogMaintenanceRouter implements Disposable {
  readonly #lc: LogContext;
  readonly #source: Worker;
  readonly #target: Worker | undefined;
  readonly #pending = new Set<string>();
  #closed = false;

  constructor(lc: LogContext, source: Worker, target: Worker | undefined) {
    this.#lc = lc.withContext(
      'component',
      'sqlite-change-log-maintenance-router',
    );
    this.#source = source;
    this.#target = target;
    source.on('message', this.#onSourceMessage);
    source.on('close', this.#onSourceClose);
    if (target) {
      target.on('message', this.#onTargetMessage);
      target.on('close', this.#onTargetClose);
      target.on('error', this.#onTargetError);
    }
  }

  readonly #onSourceMessage = (data: unknown) => {
    if (!isSQLiteChangeLogMaintenanceRequest(data)) {
      return;
    }

    let requestID = requestIDFrom(data[1]);
    let request: SQLiteChangeLogMaintenanceRequest[1];
    try {
      request = validateSQLiteChangeLogMaintenanceRequest(data[1]);
      requestID = request.requestID;
      if (this.#closed) {
        throw new Error('SQLite change-log maintenance router is closed');
      }
      if (!this.#target) {
        throw new Error('canonical SQLite change-log writer is unavailable');
      }
      if (this.#pending.has(requestID)) {
        throw new Error(
          `duplicate SQLite change-log maintenance request ${requestID}`,
        );
      }
    } catch (error) {
      this.#respondWithError(requestID, error);
      return;
    }

    this.#pending.add(requestID);
    try {
      this.#target.send<SQLiteChangeLogMaintenanceRequest>(
        ['sqliteChangeLogMaintenanceRequest', request],
        undefined,
        error => {
          if (error !== null && this.#pending.delete(requestID)) {
            this.#respondWithError(requestID, error);
          }
        },
      );
    } catch (error) {
      if (this.#pending.delete(requestID)) {
        this.#respondWithError(requestID, error);
      }
    }
  };

  readonly #onTargetMessage = (data: unknown) => {
    if (!isSQLiteChangeLogMaintenanceResponse(data)) {
      return;
    }

    let requestID = requestIDFrom(data[1]);
    let response: SQLiteChangeLogMaintenanceResponse[1];
    try {
      response = validateSQLiteChangeLogMaintenanceResponse(data[1]);
      requestID = response.requestID;
    } catch (error) {
      if (this.#pending.delete(requestID)) {
        this.#respondWithError(requestID, error);
      } else {
        this.#lc.warn?.(
          'invalid SQLite change-log maintenance response',
          error,
        );
      }
      return;
    }

    if (!this.#pending.delete(requestID)) {
      this.#lc.warn?.(
        `ignoring unexpected SQLite change-log maintenance response ${requestID}`,
      );
      return;
    }
    this.#sendToSource(response);
  };

  readonly #onTargetClose = () => {
    this.#failAll(new Error('canonical SQLite change-log writer closed'));
  };

  readonly #onTargetError = (error: Error) => {
    this.#failAll(error);
  };

  readonly #onSourceClose = () => {
    this.#pending.clear();
  };

  #respondWithError(requestID: string, error: unknown): void {
    this.#sendToSource({requestID, error: serializeError(error)});
  }

  #sendToSource(response: SQLiteChangeLogMaintenanceResponse[1]): void {
    try {
      this.#source.send<SQLiteChangeLogMaintenanceResponse>([
        'sqliteChangeLogMaintenanceResponse',
        response,
      ]);
    } catch (error) {
      this.#lc.warn?.(
        `unable to return SQLite change-log maintenance response ${response.requestID}`,
        error,
      );
    }
  }

  #failAll(error: unknown): void {
    for (const requestID of this.#pending) {
      this.#respondWithError(requestID, error);
    }
    this.#pending.clear();
  }

  close(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#failAll(new Error('SQLite change-log maintenance router closed'));
    this.#source.off('message', this.#onSourceMessage);
    this.#source.off('close', this.#onSourceClose);
    if (this.#target) {
      this.#target.off('message', this.#onTargetMessage);
      this.#target.off('close', this.#onTargetClose);
      this.#target.off('error', this.#onTargetError);
    }
  }

  [Symbol.dispose](): void {
    this.close();
  }
}

function requestIDFrom(value: unknown): string {
  return value &&
    typeof value === 'object' &&
    'requestID' in value &&
    typeof value.requestID === 'string' &&
    value.requestID.length > 0
    ? value.requestID
    : 'invalid-request';
}
