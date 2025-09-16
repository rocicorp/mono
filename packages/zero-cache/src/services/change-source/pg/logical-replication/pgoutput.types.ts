/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
