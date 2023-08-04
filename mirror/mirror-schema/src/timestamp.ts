import * as v from 'shared/src/valita.js';

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
