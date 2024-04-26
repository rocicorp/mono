import * as v from 'shared/src/valita.js';

export const entityIDSchema = v.record(v.string());

export type EntityID = v.Infer<typeof entityIDSchema>;
