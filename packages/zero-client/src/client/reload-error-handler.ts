import type {LogContext} from '@rocicorp/logger';

export const RELOAD_REASON_STORAGE_KEY = '_zeroReloadReason';

// TODO: This should get pushed down into Replicache and used for reloads we
// do there.
export function reloadWithReason(
  lc: LogContext,
  reload: () => void,
  reason: string,
) {
  if (typeof localStorage === 'undefined') {
    lc.error?.('Zero reloaded the page.', reason);
  } else {
    localStorage[RELOAD_REASON_STORAGE_KEY] = reason;
  }
  reload();
}

export function reportReloadReason(lc: LogContext) {
  if (typeof localStorage === 'undefined') {
    return;
  }
  const reason = localStorage[RELOAD_REASON_STORAGE_KEY];
  if (reason) {
    delete localStorage[RELOAD_REASON_STORAGE_KEY];
    lc.error?.('Zero reloaded the page.', reason);
  }
}
