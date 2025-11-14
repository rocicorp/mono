import type {LogContext} from '@rocicorp/logger';
import * as v from '../../../shared/src/valita.ts';
import type {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import {errorKindSchema} from '../../../zero-protocol/src/error.ts';
import {updateNeededReasonTypeSchema} from './options.ts';
import type {UpdateNeededReasonType} from './update-needed-reason-type.ts';

export const RELOAD_REASON_STORAGE_KEY = '_zeroReloadReason';
export const RELOAD_BACKOFF_STATE_KEY = '_zeroReloadBackoffState';

const reloadReasonSchema = v.tuple([
  v.union(updateNeededReasonTypeSchema, errorKindSchema),
  v.string(),
]);

const backoffStateSchema = v.object({
  lastReloadTime: v.number().default(0),
  nextIntervalMs: v.number().default(0),
});

export type BackoffState = v.Infer<typeof backoffStateSchema>;

export const MIN_RELOAD_INTERVAL_MS = 500;
export const MAX_RELOAD_INTERVAL_MS = 60_000;

// For the fraction of browsers that do not support sessionStorage.
export const FALLBACK_RELOAD_INTERVAL_MS = 10_000;

let reloadTimer: ReturnType<typeof setTimeout> | null = null;

// TODO: This should get pushed down into Replicache and used for reloads we
// do there.
export function reloadWithReason(
  lc: LogContext,
  reload: () => void,
  reason: UpdateNeededReasonType | ErrorKind,
  message: string,
) {
  if (reloadTimer) {
    lc.info?.('reload timer already scheduled');
    return;
  }
  const now = Date.now();
  const backoff = nextBackoff(lc, now);

  // Record state immediately so that it persists if the user manually reloads first.
  if (typeof sessionStorage !== 'undefined') {
    sessionStorage.setItem(RELOAD_BACKOFF_STATE_KEY, JSON.stringify(backoff));
    sessionStorage.setItem(
      RELOAD_REASON_STORAGE_KEY,
      JSON.stringify([reason, message]),
    );
  }

  const delay = backoff.lastReloadTime - now;
  lc.error?.(
    reason,
    '\n',
    'reloading',
    delay > 0 ? `in ${delay / 1000} seconds` : '',
  );
  reloadTimer = setTimeout(() => {
    reloadTimer = null;
    reload();
  }, delay);
}

export function reportReloadReason(lc: LogContext) {
  if (typeof sessionStorage !== 'undefined') {
    const value = sessionStorage.getItem(RELOAD_REASON_STORAGE_KEY);
    if (value) {
      sessionStorage.removeItem(RELOAD_REASON_STORAGE_KEY);
      try {
        const parsed = JSON.parse(value);
        const [reasonType, message] = v.parse(parsed, reloadReasonSchema);
        lc.error?.(reasonType, 'Zero reloaded the page.', message);
      } catch (e) {
        lc.error?.('Zero reloaded the page.', e);
        // ignore if not able to parse
        return;
      }
    }
  }
}

/** If a reload is scheduled, do not attempt to reconnect. */
export function reloadScheduled() {
  return reloadTimer !== null;
}

/** Call upon a successful connection, indicating that backoff should be reset. */
export function resetBackoff() {
  if (typeof sessionStorage !== 'undefined') {
    sessionStorage.removeItem(RELOAD_BACKOFF_STATE_KEY);
  }
}

function nextBackoff(lc: LogContext, now: number): BackoffState {
  if (typeof sessionStorage === 'undefined') {
    lc.warn?.(
      `sessionStorage not supported. backing off in ${
        FALLBACK_RELOAD_INTERVAL_MS / 1000
      } seconds`,
    );
    return {
      lastReloadTime: now + FALLBACK_RELOAD_INTERVAL_MS,
      nextIntervalMs: MIN_RELOAD_INTERVAL_MS,
    };
  }
  const val = sessionStorage.getItem(RELOAD_BACKOFF_STATE_KEY);
  if (!val) {
    return {lastReloadTime: now, nextIntervalMs: MIN_RELOAD_INTERVAL_MS};
  }
  let parsed: BackoffState;
  try {
    parsed = v.parse(JSON.parse(val), backoffStateSchema, 'passthrough');
  } catch (e) {
    lc.warn?.('ignoring unparsable backoff state', val, e);
    return {lastReloadTime: now, nextIntervalMs: MIN_RELOAD_INTERVAL_MS};
  }
  const {lastReloadTime, nextIntervalMs} = parsed;

  // Backoff state might not have been cleared. Reset for sufficiently old state.
  if (now - lastReloadTime > MAX_RELOAD_INTERVAL_MS * 2) {
    return {lastReloadTime: now, nextIntervalMs: MIN_RELOAD_INTERVAL_MS};
  }
  if (now < lastReloadTime) {
    // If the user manually reloaded, stick to the existing schedule.
    return parsed;
  }
  const nextReloadTime = Math.max(now, lastReloadTime + nextIntervalMs);
  return {
    lastReloadTime: nextReloadTime,
    nextIntervalMs: Math.min(nextIntervalMs * 2, MAX_RELOAD_INTERVAL_MS),
  };
}
