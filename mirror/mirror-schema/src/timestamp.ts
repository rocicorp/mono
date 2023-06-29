import * as v from 'shared/src/valita.js';

// https://firebase.google.com/docs/reference/node/firebase.firestore.Timestamp
export const timestampSchema = v.object({
  nanoseconds: v.number(),
  seconds: v.number(),
});

export type Timestamp = v.Infer<typeof timestampSchema>;
