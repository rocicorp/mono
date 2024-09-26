import {Pgoutput} from 'pg-logical-replication';
import type {JSONObject} from 'zero-cache/src/types/bigint-json.js';
import type {Satisfies} from 'zero-cache/src/types/satisfies.js';

export type MessageBegin = {
  tag: 'begin';
};

export type MessageCommit = {
  tag: 'commit';
};

// Omit the `parser: (raw: any) => any;` field from the RelationColumn,
// which does not serialize to JSON.
export type RelationColumn = Omit<Pgoutput.RelationColumn, 'parser'>;

export type MessageRelation = Omit<Pgoutput.MessageRelation, 'columns'> & {
  columns: RelationColumn[];
};

export type MessageInsert = Omit<Pgoutput.MessageInsert, 'relation'> & {
  relation: MessageRelation;
};

export type MessageUpdate = Omit<Pgoutput.MessageUpdate, 'relation'> & {
  relation: MessageRelation;
};

export type MessageDelete = Omit<Pgoutput.MessageDelete, 'relation'> & {
  relation: MessageRelation;
};

export type MessageTruncate = Omit<Pgoutput.MessageTruncate, 'relations'> & {
  relations: MessageRelation[];
};

export type DataChange = Satisfies<
  JSONObject, // guarantees serialization over IPC or network
  MessageInsert | MessageUpdate | MessageDelete | MessageTruncate
>;

export type Change = MessageBegin | DataChange | MessageCommit;

export type ChangeTag = Change['tag'];
