import * as v from 'shared/src/valita.js';

// https://firebase.google.com/docs/reference/node/firebase.firestore.Timestamp
export const timestampSchema = v.object({
  nanoseconds: v.number(),
  seconds: v.number(),
});

export type Timestamp = v.Infer<typeof timestampSchema>;

export function toMillis(timestamp: Timestamp): number {
  // Same logic as https://github.com/googleapis/nodejs-firestore/blob/ac35b372faf32f093d83af18d487f1b3f23ee673/dev/src/timestamp.ts#L242
  // Note that we avoid directly importing the server-side code in order to keep the
  // client side libraries (e.g. reflect-cli) free of server-sdk dependencies.
  return (
    timestamp.seconds * 1000 + Math.round(timestamp.nanoseconds / 1_000_000)
  );
}
