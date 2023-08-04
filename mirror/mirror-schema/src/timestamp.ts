import * as v from 'shared/src/valita.js';

// https://firebase.google.com/docs/reference/node/firebase.firestore.Timestamp
export const timestampSchema = v.object({
  nanoseconds: v.number(),
  seconds: v.number(),

  // Undocumented fields.
  _nanoseconds: v.number().optional(),
  _seconds: v.number().optional(),
});

export type Timestamp = v.Infer<typeof timestampSchema>;
