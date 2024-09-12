import {Pgoutput} from 'pg-logical-replication';
import {JSONValue} from 'shared/src/json.js';

export type MessageBegin = {
  tag: 'begin';

  // Upstream-specific fields should be preserved but ignored.
  [field: string]: JSONValue;
};

export type MessageCommit = {
  tag: 'commit';

  // Upstream-specific fields should be preserved but ignored.
  [field: string]: JSONValue;
};

/**
 * For now, a Change is a subset of the message types sent in the Postgres
 * logical replication stream. This can be augmented (e.g. to include schema
 * changes) or generalized in the future.
 */
export type Change =
  | MessageBegin
  | Pgoutput.MessageInsert
  | Pgoutput.MessageUpdate
  | Pgoutput.MessageDelete
  | Pgoutput.MessageTruncate
  | MessageCommit;
