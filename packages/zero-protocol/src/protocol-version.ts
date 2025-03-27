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
// -- Version 12 adds 'timestamp' and 'date' types to the ClientSchema ValueType. (0.18)
export const PROTOCOL_VERSION = 12;

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
export const MIN_SERVER_SUPPORTED_SYNC_PROTOCOL = 6;

assert(MIN_SERVER_SUPPORTED_SYNC_PROTOCOL < PROTOCOL_VERSION);
