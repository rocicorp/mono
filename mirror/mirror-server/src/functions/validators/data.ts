import type {DocumentSnapshot} from 'firebase-admin/firestore';
import {FunctionsErrorCode, HttpsError} from 'firebase-functions/v2/https';
import {must} from 'shared/src/must.js';

export function getDataOrFail<T>(
  snapshot: DocumentSnapshot<T>,
  code: FunctionsErrorCode,
  msg: string,
): T {
  if (!snapshot.exists) {
    throw new HttpsError(code, msg);
  }
  return must(snapshot.data());
}
