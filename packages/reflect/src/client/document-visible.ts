import type {LogContext} from '@rocicorp/logger';

type PartialDocument = Pick<
  Document,
  'visibilityState' | 'addEventListener' | 'removeEventListener'
>;

/**
 * @returns A promise that resolves when the document becomes visible.
 * If the document is already visible the promise resolves immediately.
 */
export function waitForVisible(
  lc: LogContext,
  doc: PartialDocument | undefined,
): Promise<void> {
  // No document in service worker etc. Treat context as always visible.
  if (!doc || doc.visibilityState === 'visible') {
    lc.debug?.(
      doc
        ? 'Context already visible'
        : 'No document. Treating context as always visible',
    );
    return Promise.resolve();
  }

  lc.debug?.('Waiting for context to become visible');
  return new Promise(resolve => {
    const listener = () => {
      if (doc.visibilityState === 'visible') {
        doc.removeEventListener('visibilitychange', listener);
        resolve();
      }
    };
    doc.addEventListener('visibilitychange', listener);
  });
}

/**
 * @param lc The log context to use.
 * @param ms The number of milliseconds to wait after the document becomes
 * hidden before resolving the promise.
 * @returns A promise that resolves when the document becomes hidden and stays hidden
 * for at least `ms` milliseconds.
 */
export function waitForHidden(
  lc: LogContext,
  doc: PartialDocument | undefined,
  ms: number,
): Promise<void> {
  if (!doc) {
    lc.debug?.(
      'No document. Treating context as always visible and never resolve this promise',
    );
    // This promise will never resolve.
    return new Promise(() => undefined);
  }

  return new Promise(resolve => {
    let id: ReturnType<typeof setTimeout> | undefined;
    const listener = () => {
      if (doc.visibilityState === 'hidden') {
        id = setTimeout(() => {
          doc.removeEventListener('visibilitychange', listener);
          resolve();
        }, ms);
      } else {
        clearTimeout(id);
      }
    };
    doc.addEventListener('visibilitychange', listener);
    listener();
  });
}
