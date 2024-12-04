/**
 * The `PROTOCOL_VERSION` encompasses both the wire-protocol of the `/sync/...`
 * connection between the browser and `zero-cache`, as well as the format of
 * the `AST` objects stored in both components (i.e. IDB and CVR).
 *
 * A change in the `AST` schema (e.g. new functionality added) must be
 * accompanied by an increment of the `PROTOCOL_VERSION` and a new major
 * release. The server (`zero-cache`) must be deployed before clients start
 * running the new code.
 *
 * The contract for backwards compatibility is that a `zero-cache` supports
 * its current `PROTOCOL_VERSION` and the previous one (i.e.
 * `PROTOCOL_VERSION - 1`, which is necessary to support old clients when
 * the server is rolled out). This corresponds to supporting clients running
 * the current release and the previous (major) release. Any client connections
 * from earlier protocol versions are closed with a `VersionNotSupported`
 * error.
 */
export const PROTOCOL_VERSION = 1;
