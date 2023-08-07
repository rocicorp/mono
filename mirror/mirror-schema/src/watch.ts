import type {FirestoreError, SnapshotListenOptions} from 'firebase/firestore';
import {Queue} from 'shared/src/queue.js';

/**
 * Returns an AsyncIterable of snapshots, streamed when the document or query results
 * change. The advantage of using this over the onSnapshot() API is that processing of
 * snapshots is guaranteed to be serialized (i.e. callbacks cannot be invoked
 * while the previous one is still running), and the cleanup of the snapshot subscription
 * is guaranteed without requiring a `try/finally` block.
 *
 * @param timeoutMs causes the iteration to abort with a {@link TimeoutError} after the timeout.
 */
export function watch<Snapshot>(
  docOrQuery:
    | ServerDocumentReferenceOrQuery<Snapshot>
    | WebDocumentReferenceOrQuery<Snapshot>,
  timeoutMs?: number,
): AsyncIterable<Snapshot> {
  return {
    [Symbol.asyncIterator]: () => {
      const snapshots = new Queue<Snapshot>();
      const unsubscribe = docOrQuery.onSnapshot(
        snapshot => {
          void snapshots.enqueue(snapshot);
        },
        err => {
          void snapshots.enqueueRejection(err);
        },
      );
      const timeoutID =
        timeoutMs === undefined
          ? undefined
          : setTimeout(
              () => snapshots.enqueueRejection(new TimeoutError(timeoutMs)),
              timeoutMs,
            );

      function cleanup() {
        unsubscribe();
        clearTimeout(timeoutID);
      }

      return {
        next: async () => {
          try {
            const value = await snapshots.dequeue();
            return {value};
          } catch (e) {
            cleanup();
            throw e;
          }
        },
        return: value => {
          cleanup();
          return Promise.resolve({value, done: true});
        },
      };
    },
  };
}

/** Thrown if the `watch()` iterator is not exited within its optional `timeoutMs`. */
export class TimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`Timed out after ${timeoutMs} milliseconds`);
  }
}

// Server SDK methods common to DocumentReference and Query.
interface ServerDocumentReferenceOrQuery<Snapshot> {
  onSnapshot(
    onNext: (snapshot: Snapshot) => void,
    onError?: (error: Error) => void,
  ): () => void;
}

// Web SDK methods common to DocumentReference and Query.
interface WebDocumentReferenceOrQuery<Snapshot> {
  onSnapshot(
    onNext: (snapshot: Snapshot) => void,
    onError?: (error: FirestoreError) => void,
    onCompletion?: () => void,
  ): () => void;
  onSnapshot(
    options: SnapshotListenOptions,
    onNext: (snapshot: Snapshot) => void,
    onError?: (error: FirestoreError) => void,
    onCompletion?: () => void,
  ): () => void;
}
