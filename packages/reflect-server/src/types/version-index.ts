import * as valita from 'shared/valita.js';

export const versionIndexSchemaVersion = 0;
export const versionIndexMetaSchema = valita.object({
  schemaVersion: valita.number(),
});

/**
 * The "root" entry of the index contains metadata (e.g. schema version)
 * of the index itself. Index entries are keyed with this root as a prefix,
 * e.g. `versions/0512345/fookey'
 */
export const versionIndexMetaKey = 'versions';

export type VersionIndexMeta = valita.Infer<typeof versionIndexMetaSchema>;
