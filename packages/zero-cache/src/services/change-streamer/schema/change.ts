import {Pgoutput} from 'pg-logical-replication';

/**
 * For now, a Change is a subset of the message types sent in the Postgres
 * logical replication stream. This can be augmented (e.g. to include schema
 * changes) or generalized in the future.
 */
export type Change =
  | Pgoutput.MessageBegin
  | Pgoutput.MessageCommit
  | Pgoutput.MessageInsert
  | Pgoutput.MessageUpdate
  | Pgoutput.MessageDelete
  | Pgoutput.MessageTruncate
  | Pgoutput.MessageCommit;
