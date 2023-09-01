import type {
  DocumentData,
  FirestoreDataConverter,
  QueryDocumentSnapshot,
} from '@google-cloud/firestore';
import type firebase from 'firebase/compat/app';
import * as v from 'shared/src/valita.js';

export function firestoreDataConverter<T extends DocumentData>(
  schema: v.Type<T>,
): DataConverter<T> {
  return new DataConverter(schema);
}

export class DataConverter<T extends DocumentData>
  implements
    FirestoreDataConverter<T>,
    firebase.default.firestore.FirestoreDataConverter<T>
{
  readonly #schema: v.Type<T>;
  readonly #mode: v.ParseOptionsMode;

  constructor(schema: v.Type<T>, mode: v.ParseOptionsMode = 'passthrough') {
    this.#schema = schema;
    this.#mode = mode;
  }

  strict(): DataConverter<T> {
    return new DataConverter(this.#schema, 'strict');
  }

  toFirestore(obj: T): DocumentData {
    return obj;
  }

  fromFirestore(snapshot: QueryDocumentSnapshot): T;
  fromFirestore(
    snapshot: firebase.default.firestore.QueryDocumentSnapshot,
    options: firebase.default.firestore.SnapshotOptions,
  ): T;

  fromFirestore(
    snapshot:
      | QueryDocumentSnapshot
      | firebase.default.firestore.QueryDocumentSnapshot,
    _?: firebase.default.firestore.SnapshotOptions,
  ): T {
    return v.parse(snapshot.data(), this.#schema, this.#mode);
  }
}
