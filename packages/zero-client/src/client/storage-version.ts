/**
 * The version of what a client stores in its local database. It is part of
 * the database's name, so a new number opens a new, empty database. The old
 * one is left behind with any mutations it hadn't pushed, because mutation
 * recovery is off.
 *
 * Bump it when one version of the client can't read what another version
 * stored, in either direction: a newer client opens an older client's
 * database on upgrade, and an older client opens a newer one's on rollback.
 * `storage-version.test.ts` fails when what a client stores changes.
 *
 * The rest of the name covers the app's schema (the hash from
 * `clientSchemaFrom`) and Replicache's own layout (`FormatVersion`).
 *
 * This is separate from `PROTOCOL_VERSION`, which changes with the wire
 * format. It starts at 53, the protocol version the name used until then, so
 * that no client moved to a new database.
 */
export const STORAGE_VERSION = 53;
