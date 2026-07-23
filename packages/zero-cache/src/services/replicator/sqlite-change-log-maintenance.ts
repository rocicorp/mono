import {pid} from 'node:process';
import {assert, assertObject} from '../../../../shared/src/asserts.ts';
import type {Worker} from '../../types/processes.ts';
import type {SQLiteChangeLogPurgeResult} from './sqlite-change-log-purger.ts';
import {deserializeError, type SerializedError} from './write-worker-client.ts';

export type SQLiteChangeLogMaintenance = {
  /** Backup/subscriber/head safety floor. Rows at this watermark are kept. */
  readonly safeFloor: string;
  /** Wall-clock time used to derive the retention cutoff in the writer. */
  readonly requestTimeMs: number;
  readonly retentionMs: number;
  readonly maxRows: number;
};

export type SQLiteChangeLogMaintenanceRequestPayload =
  SQLiteChangeLogMaintenance & {
    readonly requestID: string;
  };

export type SQLiteChangeLogMaintenanceResponsePayload = {
  readonly requestID: string;
  readonly result?: SQLiteChangeLogPurgeResult | undefined;
  readonly error?: SerializedError | undefined;
};

export type SQLiteChangeLogMaintenanceRequest = [
  'sqliteChangeLogMaintenanceRequest',
  SQLiteChangeLogMaintenanceRequestPayload,
];

export type SQLiteChangeLogMaintenanceResponse = [
  'sqliteChangeLogMaintenanceResponse',
  SQLiteChangeLogMaintenanceResponsePayload,
];

export class SQLiteChangeLogMaintenanceTimeoutError extends Error {}

export const SQLITE_CHANGE_LOG_MAINTENANCE_TIMEOUT_MS = 30_000;

let nextRequestID = 0;

/**
 * Sends one request from the change-streamer process to the dispatcher. The
 * dispatcher relays it to the canonical replicator and routes the response
 * back to this worker.
 */
export function requestSQLiteChangeLogMaintenance(
  worker: Worker,
  maintenance: SQLiteChangeLogMaintenance,
  timeoutMs: number,
): Promise<SQLiteChangeLogPurgeResult> {
  validateSQLiteChangeLogMaintenance(maintenance);
  assert(
    Number.isSafeInteger(timeoutMs) && timeoutMs > 0,
    'SQLite change-log maintenance timeout must be a positive safe integer',
  );

  const requestID = `${pid}-${++nextRequestID}`;
  const payload = {...maintenance, requestID};
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(
        new SQLiteChangeLogMaintenanceTimeoutError(
          `timed out waiting ${timeoutMs} ms for SQLite change-log maintenance`,
        ),
      );
    }, timeoutMs);

    const onMessage = (data: unknown) => {
      if (!isMessageType(data, 'sqliteChangeLogMaintenanceResponse')) {
        return;
      }
      let response: SQLiteChangeLogMaintenanceResponsePayload;
      try {
        response = validateSQLiteChangeLogMaintenanceResponse(data[1]);
      } catch (error) {
        cleanup();
        reject(error);
        return;
      }
      if (response.requestID !== requestID) {
        return;
      }
      cleanup();
      if (response.error !== undefined) {
        reject(deserializeError(response.error));
      } else {
        resolve(response.result as SQLiteChangeLogPurgeResult);
      }
    };

    const onClose = () => {
      cleanup();
      reject(
        new Error(
          'dispatcher closed during SQLite change-log maintenance request',
        ),
      );
    };

    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };

    const cleanup = () => {
      clearTimeout(timeout);
      worker.off('message', onMessage);
      worker.off('close', onClose);
      worker.off('error', onError);
    };

    worker.on('message', onMessage);
    worker.once('close', onClose);
    worker.once('error', onError);
    try {
      worker.send<SQLiteChangeLogMaintenanceRequest>(
        ['sqliteChangeLogMaintenanceRequest', payload],
        undefined,
        error => {
          if (error !== null) {
            cleanup();
            reject(error);
          }
        },
      );
    } catch (error) {
      cleanup();
      reject(error);
    }
  });
}

export function validateSQLiteChangeLogMaintenanceRequest(
  value: unknown,
): SQLiteChangeLogMaintenanceRequestPayload {
  assertObject(value);
  const {requestID, safeFloor, requestTimeMs, retentionMs, maxRows} = value;
  assert(
    typeof requestID === 'string' && requestID.length > 0,
    'SQLite change-log maintenance requestID must be a non-empty string',
  );
  const maintenance = {safeFloor, requestTimeMs, retentionMs, maxRows};
  validateSQLiteChangeLogMaintenance(maintenance);
  return {requestID, ...maintenance};
}

export function validateSQLiteChangeLogMaintenanceResponse(
  value: unknown,
): SQLiteChangeLogMaintenanceResponsePayload {
  assertObject(value);
  const {requestID, result, error} = value;
  assert(
    typeof requestID === 'string' && requestID.length > 0,
    'SQLite change-log maintenance response requestID must be a non-empty string',
  );
  assert(
    (result === undefined) !== (error === undefined),
    'SQLite change-log maintenance response must contain exactly one of result or error',
  );
  if (error !== undefined) {
    validateSerializedError(error);
    return {requestID, error};
  }
  return {
    requestID,
    result: validatePurgeResult(result),
  };
}

export function isSQLiteChangeLogMaintenanceRequest(
  data: unknown,
): data is ['sqliteChangeLogMaintenanceRequest', unknown] {
  return isMessageType(data, 'sqliteChangeLogMaintenanceRequest');
}

export function isSQLiteChangeLogMaintenanceResponse(
  data: unknown,
): data is ['sqliteChangeLogMaintenanceResponse', unknown] {
  return isMessageType(data, 'sqliteChangeLogMaintenanceResponse');
}

export function validateSQLiteChangeLogMaintenance(
  value: unknown,
): asserts value is SQLiteChangeLogMaintenance {
  assertObject(value);
  assert(
    typeof value.safeFloor === 'string' && value.safeFloor.length > 0,
    'SQLite change-log maintenance safe floor must be a non-empty string',
  );
  assert(
    Number.isSafeInteger(value.requestTimeMs) &&
      (value.requestTimeMs as number) >= 0,
    'SQLite change-log maintenance request time must be a non-negative safe integer',
  );
  assertPositiveSafeInteger(value.retentionMs, 'retention');
  assertPositiveSafeInteger(value.maxRows, 'batch size');
}

function validatePurgeResult(value: unknown): SQLiteChangeLogPurgeResult {
  assertObject(value);
  const {
    headWatermark,
    timeFloor,
    effectiveFloor,
    deletedRows,
    deletedBeforeWatermark,
    moreEligible,
  } = value;
  for (const [name, watermark] of [
    ['head', headWatermark],
    ['time floor', timeFloor],
    ['effective floor', effectiveFloor],
  ] as const) {
    assert(
      typeof watermark === 'string' && watermark.length > 0,
      `SQLite change-log maintenance ${name} must be a non-empty string`,
    );
  }
  assert(
    Number.isSafeInteger(deletedRows) && (deletedRows as number) >= 0,
    'SQLite change-log maintenance deleted row count must be a non-negative safe integer',
  );
  assert(
    deletedBeforeWatermark === undefined ||
      (typeof deletedBeforeWatermark === 'string' &&
        deletedBeforeWatermark.length > 0),
    'SQLite change-log maintenance deleted-before watermark must be a non-empty string',
  );
  assert(
    typeof moreEligible === 'boolean',
    'SQLite change-log maintenance more-eligible flag must be a boolean',
  );
  return {
    headWatermark,
    timeFloor,
    effectiveFloor,
    deletedRows,
    deletedBeforeWatermark,
    moreEligible,
  } as SQLiteChangeLogPurgeResult;
}

function validateSerializedError(
  value: unknown,
  depth = 0,
): asserts value is SerializedError {
  assert(depth < 20, 'SQLite change-log maintenance error cause is too deep');
  assertObject(value);
  assert(
    typeof value.name === 'string' && typeof value.message === 'string',
    'SQLite change-log maintenance error must contain a name and message',
  );
  assert(
    value.stack === undefined || typeof value.stack === 'string',
    'SQLite change-log maintenance error stack must be a string',
  );
  assert(
    value.details === undefined ||
      (typeof value.details === 'object' && value.details !== null),
    'SQLite change-log maintenance error details must be an object',
  );
  if (value.cause !== undefined && typeof value.cause !== 'string') {
    validateSerializedError(value.cause, depth + 1);
  }
}

function assertPositiveSafeInteger(value: unknown, name: string): void {
  assert(
    Number.isSafeInteger(value) && (value as number) > 0,
    `SQLite change-log maintenance ${name} must be a positive safe integer`,
  );
}

function isMessageType<T extends string>(
  data: unknown,
  type: T,
): data is [T, unknown] {
  return Array.isArray(data) && data.length === 2 && data[0] === type;
}
