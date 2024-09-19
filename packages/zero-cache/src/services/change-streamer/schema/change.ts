import {Pgoutput} from 'pg-logical-replication';

export type MessageBegin = {
  tag: 'begin';
};

export type MessageCommit = {
  tag: 'commit';
};

export type DataChange =
  | Pgoutput.MessageInsert
  | Pgoutput.MessageUpdate
  | Pgoutput.MessageDelete
  | Pgoutput.MessageTruncate;

export type Change = MessageBegin | DataChange | MessageCommit;

export type ChangeTag = Change['tag'];
