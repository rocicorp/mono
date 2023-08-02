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
  implements FirestoreDataConverter<T>
{
  #schema: v.Type<T>;
  readonly forClient: firebase.default.firestore.FirestoreDataConverter<T>;

  constructor(schema: v.Type<T>) {
    this.#schema = schema;
    this.forClient = new ClientImpl(schema);
  }

  toFirestore(obj: T): DocumentData {
    return obj;
  }

  fromFirestore(snapshot: QueryDocumentSnapshot): T {
    return v.parse(snapshot.data(), this.#schema);
  }
}

class ClientImpl<T extends DocumentData>
  implements firebase.default.firestore.FirestoreDataConverter<T>
{
  #schema: v.Type<T>;

  constructor(schema: v.Type<T>) {
    this.#schema = schema;
  }

  toFirestore(obj: T): DocumentData;
  toFirestore(
    obj: Partial<T>,
    options: firebase.default.firestore.SetOptions,
  ): DocumentData;
  toFirestore(
    obj: Partial<T>,
    _?: firebase.default.firestore.SetOptions,
  ): DocumentData {
    return obj;
  }

  fromFirestore(
    snapshot: firebase.default.firestore.QueryDocumentSnapshot,
    _: firebase.default.firestore.SnapshotOptions,
  ): T {
    return v.parse(snapshot.data(), this.#schema);
  }
}
