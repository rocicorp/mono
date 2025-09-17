/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members, @typescript-eslint/prefer-promise-reject-errors */
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';

// TODO(arv): Unify with src/scan-options.ts

// How to use ScanOptions. This could be simpler if we added more structure, eg
// separate scan types for regular vs index scans, but opting instead for
// simpler structure at the cost of making it slightly harder to hold.
//
// For *all* scans:
// - limit: only return at most this many matches
//
// For *regular* scans:
// - prefix: (primary) key prefix to scan, "" matches all of them
// - start_key: start returning (primary key) matches from this value, inclusive
//   unless:
// - start_exclusive: start returning matches *after* the start_key
// - start_key can be used for pagination
//
// For *index* scans:
// - index_name: name of the index to use
// - prefix: *secondary* key prefix to scan for, "" matches all of them
// - start_secondary_key: start returning *secondary* key matches from this
//   value, AND:
// - start_key: if provided start matching on EXACTLY the start_secondary_key
//   and return *primary* key matches starting from this value (empty string
//   means all of them).
// - start_exclusive: start returning matches *after* the (start_secondary_key,
//   start_key) entry; exclusive covers both
// - start_secondary_key and start_key can be used for pagination
//
// NOTE that in above for index scans if you provide Some start_key, the
// secondary_index_key is treated as an exact match.
export type ScanOptions = {
  prefix?: string | undefined;
  startSecondaryKey?: string | undefined;
  startKey?: string | undefined;
  startExclusive?: boolean | undefined;
  limit?: number | undefined;
  indexName?: string | undefined;
};

export type ScanItem = {
  primaryKey: string;
  secondaryKey: string;
  val: ReadonlyJSONValue;
};
