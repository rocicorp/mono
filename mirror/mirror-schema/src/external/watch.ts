import {watch} from '../watch.js';
import {
  onSnapshot,
  DocumentReference,
  DocumentSnapshot,
  Query,
  QuerySnapshot,
} from 'firebase/firestore';

export function watchDoc<Snapshot>(
  doc: DocumentReference<Snapshot>,
  timeoutMs?: number,
): AsyncIterable<DocumentSnapshot<Snapshot>> {
  return watch(
    {
      onSnapshot: (onNext, onError, complete) =>
        onSnapshot(doc, onNext, onError, complete),
    },
    timeoutMs,
  );
}

export function watchDocs<Snapshot>(
  query: Query<Snapshot>,
  timeoutMs?: number,
): AsyncIterable<QuerySnapshot<Snapshot>> {
  return watch(
    {
      onSnapshot: (onNext, onError, complete) =>
        onSnapshot(query, onNext, onError, complete),
    },
    timeoutMs,
  );
}
