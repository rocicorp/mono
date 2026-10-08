// Feature flags let a client ask for behavior that an older server can
// ignore, without raising CLIENT_PROTOCOL_VERSION (see protocol-version.ts).
//
// The client sends its flags in one connect URL parameter (setFeatureFlags in
// connect.ts). A flag the client doesn't send means the server's default. A
// server ignores flags it doesn't know, so the client must keep supporting the
// old behavior.
//
// The numbers are part of the wire protocol: they are bit positions in that
// parameter. Never change a flag's number, and never reuse the number of a
// retired flag.

/**
 * Binary poke chunks instead of JSON `pokePart` messages. Defaults to on for
 * clients at protocol version 52 or above.
 */
export const PokeChunk = 0;

export type PokeChunk = typeof PokeChunk;

/**
 * `filter` nodes in the planner events of an analyze-query result
 * (`AnalyzeQueryResult.joinPlans`). Defaults to on for clients at protocol
 * version 53 or above.
 */
export const AnalyzeFilterNode = 1;

export type AnalyzeFilterNode = typeof AnalyzeFilterNode;
