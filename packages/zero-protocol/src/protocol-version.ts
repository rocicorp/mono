import {assert} from '../../shared/src/asserts.ts';

/**
 * The highest sync protocol version `zero-cache` accepts (i.e. the version
 * declared in the "/sync/v{#}/connect" URL). Clients send
 * {@link CLIENT_PROTOCOL_VERSION}.
 *
 * The protocol encompasses both the wire-protocol of the `/sync/...`
 * connection between the browser and `zero-cache`, as well as the format of
 * the `AST` objects stored in both components (i.e. IDB and CVR).
 *
 * A client one release ahead of the server must still be able to connect, so
 * that clients and `zero-cache` can be updated and rolled back in either
 * order. Two rules keep that working:
 *
 * 1. A change an old server can ignore goes behind a protocol flag (see
 *    `protocol-flag-enum.ts`) and the client keeps sending the same
 *    {@link CLIENT_PROTOCOL_VERSION}. Old servers ignore flags they don't
 *    know, so the client must keep supporting the old behavior.
 * 2. A change an old server can't ignore (e.g. new `AST` functionality)
 *    increments `PROTOCOL_VERSION` one release before clients start sending
 *    it in {@link CLIENT_PROTOCOL_VERSION}.
 */
// History:
// -- Version 5 adds support for `pokeEnd.cookie`. (0.14)
// -- Version 6 makes `pokeStart.cookie` optional. (0.16)
// -- Version 7 introduces the initConnection.clientSchema field. (0.17)
// -- Version 8 drops support for Version 5 (0.18).
// -- Version 11 adds inspect queries. (0.18)
// -- Version 12 adds 'timestamp' and 'date' types to the ClientSchema ValueType. (not shipped, reversed by version 14)
// -- Version 14 removes 'timestamp' and 'date' types from the ClientSchema ValueType. (0.18)
// -- Version 15 adds a `userPushParams` field to `initConnection` (0.19)
// -- Version 16 adds a new error type (alreadyProcessed) to mutation responses (0.19)
// -- Version 17 deprecates `AST` in downstream query puts. It was never used anyway. (0.21)
// -- Version 18 adds `name` and `args` to the `queries-patch` protocol (0.21)
// -- Version 19 adds `activeClients` to the `initConnection` protocol (0.22)
// -- Version 20 changes inspector down message (0.22)
// -- Version 21 removes `AST` in downstream query puts which was deprecated in Version 17, removes support for versions < 18 (0.22)
// -- Version 22 adds an optional 'userQueryParams' field to `initConnection` (0.22)
// -- Version 23 add `mutationResults` to poke (0.22)
// -- Version 24 adds `ackMutationResults` to upstream (0.22).
// -- version 25 modifies `mutationsResults` to include `del` patches (0.22)
// -- version 26 adds inspect/metrics and adds metrics to inspect/query (0.23)
// -- version 27 adds inspect/version (0.23)
// -- version 28 adds more inspect/metrics (0.23)
// -- version 29 adds error responses for custom queries (0.23)
// -- version 30 adds an optional primaryKey to the ClientSchema (0.24)
// -- version 31 adds admin password authentication to inspector RPC calls (0.24)
// -- version 32 adds analyze-query to the inspector RPC calls (0.24)
// -- version 33 adds `flip` to CorrelatedSubquery (0.25)
// -- version 34 moves `flip` from CorrelatedSubquery to CorrelatedSubqueryCondition (0.25)
// -- version 35 adds `readRows`, `readRowCountsByQuery` and `readRowCount` to analyze-query result (0.25)
// -- version 36 changes inspector analyze-query and adds error response to RPC (0.25)
// -- version 37 adds `elapsed` to AnalyzeQueryResult (0.25)
// -- version 38 adds structured push/transform error responses (0.25)
// -- version 39 removes per-transform error types and adds `message` to app error (0.25)
// -- version 40 adds `dbRowScansByQuery` to AnalyzeQueryResult (0.25)
// -- version 41 makes ClientSchema.primaryKey required (0.25)
// -- version 42 adds planner events to AnalyzeQueryResult (0.25)
// -- version 43 renames `plans` to `sqlitePlans`, `plannerEvents` to `joinPlans`, and `plannerDebug` option to `joinPlans` (0.25)
// -- version 44 adds profileID to connection URL (0.25)
// -- version 45 adds userPushHeaders and userQueryHeaders to initConnection (0.25)
// -- version 46 adds scalarSubquery condition type to AST
// -- version 47 adds optional auth token to push body
// -- version 48 adds updateAuth
// -- version 49 adds `scalar` to CorrelatedSubqueryCondition, removes scalarSubquery
// -- version 50 adds OTEL headers to push and query messages
// -- version 51 changes inspector metrics fields
// -- version 52 replaces JSON pokePart messages with binary poke chunks for
//    clients using protocol version 52 or newer. Older clients retain pokePart
//    unless they send the `PokeChunk` protocol flag. (1.10 canaries)
export const PROTOCOL_VERSION = 52;

/**
 * The sync protocol version clients send in the "/sync/v{#}/connect" URL.
 *
 * Kept below {@link PROTOCOL_VERSION} so that clients can connect to servers
 * from earlier releases: 1.9 servers accept versions up to 51. Clients ask for
 * newer behavior with protocol flags instead (rule 1 above). Only increase it
 * to a version that every supported server already accepts (rule 2 above).
 *
 * This number is also part of the client's local database name (Replicache
 * `schemaVersion`). Changing it opens a fresh local database, which drops
 * unsent mutations because Zero disables mutation recovery. Canary clients
 * already used 52 and 53 in their database names, so if the name changes it
 * must move above 53; reusing 52 or 53 would open a stale canary database.
 */
export const CLIENT_PROTOCOL_VERSION = 51;

/**
 * The minimum server-supported sync protocol version (i.e. the version
 * declared in the "/sync/v{#}/connect" URL).
 *
 * CloudZero can serve applications running old client releases indefinitely.
 * Consequently, this value must never be increased: `zero-cache` must continue
 * to support every sync protocol from this version through `PROTOCOL_VERSION`.
 * A wire change that an old client cannot ignore must branch on the client's
 * protocol version and retain the old behavior. Keep each compatibility fork
 * isolated at the boundary that owns the changed behavior and test both sides
 * of its version cutoff.
 *
 * Client connections from protocol versions before this historical floor are
 * closed with a `VersionNotSupported` error.
 */
export const MIN_SERVER_SUPPORTED_SYNC_PROTOCOL = 30;

assert(
  MIN_SERVER_SUPPORTED_SYNC_PROTOCOL <= CLIENT_PROTOCOL_VERSION &&
    CLIENT_PROTOCOL_VERSION <= PROTOCOL_VERSION,
  'CLIENT_PROTOCOL_VERSION must be between MIN_SERVER_SUPPORTED_SYNC_PROTOCOL and PROTOCOL_VERSION',
);
