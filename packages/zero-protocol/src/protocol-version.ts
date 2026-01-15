import {assert} from '../../shared/src/asserts.ts';

/**
 * The current `PROTOCOL_VERSION` of the code.
 *
 * The `PROTOCOL_VERSION` encompasses both the wire-protocol of the `/sync/...`
 * connection between the browser and `zero-cache`, as well as the format of
 * the `AST` objects stored in both components (i.e. IDB and CVR).
 *
 * A change in the `AST` schema (e.g. new functionality added) must be
 * accompanied by an increment of the `PROTOCOL_VERSION` and a new major
 * release. The server (`zero-cache`) must be deployed before clients start
 * running the new code.
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
export const PROTOCOL_VERSION = 44;

/**
 * The minimum server-supported sync protocol version (i.e. the version
 * declared in the "/sync/v{#}/connect" URL). The contract for
 * backwards compatibility is that a `zero-cache` supports the current
 * `PROTOCOL_VERSION` and at least the previous one (i.e. `PROTOCOL_VERSION - 1`)
 * if not earlier ones as well. This corresponds to supporting clients running
 * the current release and the previous (major) release. Any client connections
 * from protocol versions before `MIN_SERVER_SUPPORTED_PROTOCOL_VERSION` are
 * closed with a `VersionNotSupported` error.
 */
export const MIN_SERVER_SUPPORTED_SYNC_PROTOCOL = 30;

assert(MIN_SERVER_SUPPORTED_SYNC_PROTOCOL < PROTOCOL_VERSION);
