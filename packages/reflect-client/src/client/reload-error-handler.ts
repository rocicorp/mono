import type {LogContext} from '@rocicorp/logger';

export const RELOAD_REASON_STORAGE_KEY = '_reflectReloadReason';

// TODO: This should get pushed down into Replicache and used for reloads we
// do there.
export function reloadWithReason(
  reload: () => void,
  storage: Record<string, string>,
  reason: string,
) {
  storage[RELOAD_REASON_STORAGE_KEY] = reason;
  reload();
}

export function reportReloadReason(
  lc: LogContext,
  storage: Record<string, string>,
) {
  const reason = storage[RELOAD_REASON_STORAGE_KEY];
  if (reason) {
    delete storage[RELOAD_REASON_STORAGE_KEY];
    lc.error?.('Reflect reloaded the page.', reason);
  }
}
