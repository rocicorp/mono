import {
  createWriteWorkerAPI,
  handleWriteWorkerRequest,
  type WriteWorkerHost,
} from '../../services/replicator/write-worker-api.ts';
import {
  TransportWriteWorkerClient,
  type Response,
  type WriteError,
} from '../../services/replicator/write-worker-client.ts';
import {currentIncarnation} from './incarnation.ts';

export type InThreadWorkerHost = Omit<WriteWorkerHost, 'postWriteError'>;

/**
 * A write worker in the calling thread: the real client over a transport that
 * hands each request to the real worker API on a microtask, and each response
 * back the same way.
 *
 * Nothing crosses the transport synchronously, as nothing crosses a
 * `MessagePort`. So the client's single-flight rule is the real one, and so is
 * the worker's own `abort()`: it can arrive while `processMessages` is
 * suspended in the litestream checkpointer.
 *
 * Built inside an incarnation, the worker goes quiet when that incarnation is
 * fenced: requests are dropped and no response is delivered.
 */
export function inThreadWriteWorker(
  host: InThreadWorkerHost,
): TransportWriteWorkerClient {
  let onMessage: ((msg: Response | WriteError) => void) | undefined;
  let onExit: ((code: number) => void) | undefined;
  let stopped = false;
  currentIncarnation()?.onFence(() => {
    stopped = true;
  });

  const deliver = (msg: Response | WriteError) =>
    queueMicrotask(() => {
      if (!stopped) {
        onMessage?.(msg);
      }
    });
  const api = createWriteWorkerAPI({...host, postWriteError: deliver});

  return new TransportWriteWorkerClient({
    postMessage: msg =>
      queueMicrotask(() => {
        if (!stopped) {
          void handleWriteWorkerRequest(api, msg, deliver);
        }
      }),
    onMessage: handler => {
      onMessage = handler;
    },
    // There is no thread to fail.
    onError: () => {},
    onExit: handler => {
      onExit = handler;
    },
    terminate: () => {
      stopped = true;
      onExit?.(0);
      return Promise.resolve();
    },
  });
}
