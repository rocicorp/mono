import type {
  DocumentData,
  FirestoreDataConverter,
  QueryDocumentSnapshot,
} from '@google-cloud/firestore';
import * as v from 'shared/src/valita.js';

export function firestoreDataConverter<T extends DocumentData>(
  schema: v.Type<T>,
): FirestoreDataConverter<T> {
  return {
    toFirestore(obj: T): DocumentData {
      return obj;
    },
    fromFirestore(snapshot: QueryDocumentSnapshot): T {
      return v.parse(snapshot.data(), schema);
    },
  };
}
