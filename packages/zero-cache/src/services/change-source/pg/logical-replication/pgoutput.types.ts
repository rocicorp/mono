// Forked from https://github.com/kibae/pg-logical-replication/blob/c55abddc62eadd61bd38922037ecb7a1469fa8c3/src/output-plugins/pgoutput/pgoutput.types.ts
/* eslint-disable @typescript-eslint/no-explicit-any */

// https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS

// export interface Options {
//   protoVersion: 1 | 2
//   publicationNames: string[]
//   messages?: boolean
// }

export type Message =
  | MessageBegin
  | MessageCommit
  | MessageDelete
  | MessageInsert
  | MessageMessage
  | MessageOrigin
  | MessageRelation
  | MessageTruncate
  | MessageType
  | MessageUpdate;

export interface MessageBegin {
  tag: 'begin';
  commitLsn: string | null;
  commitTime: bigint;
  xid: number;
}

export interface MessageCommit {
  tag: 'commit';
  flags: number;
  commitLsn: string | null;
  commitEndLsn: string | null;
  commitTime: bigint;
}

export interface MessageDelete {
  tag: 'delete';
  relation: MessageRelation;
  key: Record<string, any> | null;
  old: Record<string, any> | null;
}

export interface MessageInsert {
  tag: 'insert';
  relation: MessageRelation;
  new: Record<string, any>;
}

export interface MessageMessage {
  tag: 'message';
  flags: number;
  transactional: boolean;
  messageLsn: string | null;
  prefix: string;
  content: Uint8Array;
}

export interface MessageOrigin {
  tag: 'origin';
  originLsn: string | null;
  originName: string;
}

export interface MessageRelation {
  tag: 'relation';
  relationOid: number;
  schema: string;
  name: string;
  replicaIdentity: 'default' | 'nothing' | 'full' | 'index';
  columns: RelationColumn[];
  keyColumns: string[];
}

export interface RelationColumn {
  name: string;
  flags: number;
  typeOid: number;
  typeMod: number;
  typeSchema: string | null;
  typeName: string | null;
  parser: (raw: any) => any;
}

export interface MessageTruncate {
  tag: 'truncate';
  cascade: boolean;
  restartIdentity: boolean;
  relations: MessageRelation[];
}

export interface MessageType {
  tag: 'type';
  typeOid: number;
  typeSchema: string;
  typeName: string;
}

export interface MessageUpdate {
  tag: 'update';
  relation: MessageRelation;
  key: Record<string, any> | null;
  old: Record<string, any> | null;
  new: Record<string, any>;
}
