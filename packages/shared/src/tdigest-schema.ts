import * as v from './valita.ts';

/**
 * Valita schema for TDigest JSON representation.
 * Matches the structure returned by TDigest.toJSON().
 */
export const tdigestSchema = v.tuple([v.number()]).concat(v.array(v.number()));

export type TDigestJSON = v.Infer<typeof tdigestSchema>;
