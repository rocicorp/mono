/**
 * Prefix for the synthetic table that holds the result rows of an `aggregate`
 * query/relationship. The synthetic name keeps aggregate rows distinct from real
 * child rows when routed/synced.
 *
 * The separator is `:` rather than `/` on purpose: synthetic table names must
 * contain **no `/`** so they round-trip through the client's Replicache row-key
 * format (`e/<table>/<pk>`) and `sourceNameFromKey` unchanged — those split on
 * the first `/`, which must be the table↔key delimiter. A quoted SQL
 * identifier can contain `:`, so `createSchema` rejects table names that start
 * with the prefix.
 */
export const AGGREGATE_TABLE_SEPARATOR = ':';
export const AGGREGATE_TABLE_PREFIX = `aggregate${AGGREGATE_TABLE_SEPARATOR}`;
