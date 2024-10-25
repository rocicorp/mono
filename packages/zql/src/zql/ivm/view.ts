import type {Value} from '../../../../zero-protocol/src/data.js';
import type {Input} from './operator.js';
import type {Query, QueryType} from '../query/query.js';
import type {TableSchema} from '../query/schema.js';

export type View = EntryList | Entry | undefined;
export type EntryList = Entry[];
export type Entry = {[key: string]: Value | View};

export type Format = {
  singular: boolean;
  relationships: Record<string, Format>;
};

export type ViewFactory<
  TSchema extends TableSchema,
  TReturn extends QueryType,
  T,
> = (
  query: Query<TSchema, TReturn>,
  input: Input,
  format: Format,
  onDestroy: () => void,
  onTransactionCommit: (cb: () => void) => void,
) => T;
