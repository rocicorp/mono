/**
 * The version of what a client stores in its local database. It is part of
 * the database's name, so a new number opens a new, empty database. The old
 * one is left behind with any mutations it hadn't pushed, because mutation
 * recovery is off.
 *
 * Bump it when one version of the client can't read what another version
 * stored, in either direction: a newer client opens an older client's
 * database on upgrade, and an older client opens a newer one's on rollback.
 * `storage-version.test.ts` pins what a client stores for a poke and for
 * pending mutations.
 *
 * The rest of the name covers the app's schema (the hash from
 * `clientSchemaFrom`) and Replicache's own layout (`FormatVersion`).
 *
 * This is separate from `PROTOCOL_VERSION`, which changes with the wire
 * format. It is 52, the protocol version 1.10.0 named its database with, so
 * 1.10.0 clients keep their database and the mutations they haven't pushed.
 * Protocol 53 didn't change what a client stores. 1.11 canaries from
 * 1.11.0-canary.34 on, which named their database with 53, move once.
 */
export const STORAGE_VERSION = 52;
