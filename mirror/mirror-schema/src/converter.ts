import type {
  DocumentData,
  FirestoreDataConverter,
  QueryDocumentSnapshot,
} from '@google-cloud/firestore';
import type {
  FirestoreDataConverter as ClientDataConverter,
  QueryDocumentSnapshot as ClientQueryDocumentSnapshot,
  SnapshotOptions,
} from 'firebase/firestore';
import * as v from 'shared/src/valita.js';

export function firestoreDataConverter<T extends DocumentData>(
  schema: v.Type<T>,
): DataConverter<T> {
  return new DataConverter(schema);
}

export class DataConverter<T extends DocumentData>
  implements FirestoreDataConverter<T>, ClientDataConverter<T>
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
    snapshot: ClientQueryDocumentSnapshot,
    options: SnapshotOptions,
  ): T;

  fromFirestore(
    snapshot: QueryDocumentSnapshot | ClientQueryDocumentSnapshot,
    _?: SnapshotOptions,
  ): T {
    return v.parse(snapshot.data(), this.#schema, this.#mode);
  }
}
