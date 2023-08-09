import * as v from 'shared/src/valita.js';
import {Timestamp as FirestoreTimestamp} from '@google-cloud/firestore';

// https://firebase.google.com/docs/reference/node/firebase.firestore.Timestamp
export const timestampSchema = v.object({
  nanoseconds: v.number(),
  seconds: v.number(),

  // Undocumented fields.
  // eslint-disable-next-line @typescript-eslint/naming-convention
  _nanoseconds: v.number().optional(),
  // eslint-disable-next-line @typescript-eslint/naming-convention
  _seconds: v.number().optional(),
});

export type Timestamp = v.Infer<typeof timestampSchema>;

export function toMillis(timestamp: Timestamp): number {
  return new FirestoreTimestamp(
    timestamp.seconds,
    timestamp.nanoseconds,
  ).toMillis();
}
