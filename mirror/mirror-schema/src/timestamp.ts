import * as v from 'shared/src/valita.js';

// Interface to represent both client-side and server-side versions of the Timestamp
// class.
export interface Timestamp {
  nanoseconds: number;
  seconds: number;
  toMillis(): number;
  toDate(): Date;
}

// https://firebase.google.com/docs/reference/node/firebase.firestore.Timestamp
export const timestampSchema = v
  .object({
    nanoseconds: v.number(),
    seconds: v.number(),
  })
  .chain(val => {
    const ts = val as Timestamp;
    if (typeof ts.toMillis === 'function' && typeof ts.toDate === 'function') {
      return v.ok(ts);
    }
    return v.err(`Expected Timestamp but got ${String(val)}`);
  });
