import type {LogContext} from '@rocicorp/logger';

let unhandledRejectHandlerAdded = false;

export function withUnhandledRejectionHandler<R>(
  fn: (lc: LogContext) => Promise<R>,
): (lc: LogContext) => Promise<R> {
  return lc => {
    registerUnhandledRejectionHandler(lc);
    return fn(lc);
  };
}

export function registerUnhandledRejectionHandler(lc: LogContext) {
  if (!unhandledRejectHandlerAdded) {
    addEventListener('unhandledrejection', e => {
      lc.error?.(`Unhandled promise rejection`, e.reason);
    });
    unhandledRejectHandlerAdded = true;
  }
}
