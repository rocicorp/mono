import type {Value} from '../../../zero-protocol/src/data.js';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.js';
import type {Query} from '../query/query.js';
import type {Input} from './operator.js';

export type View = EntryList | Entry | undefined;
export type EntryList = readonly Entry[];
export type Entry = {[key: string]: Value | View};

export type Format = {
  singular: boolean;
  relationships: Record<string, Format>;
};

export type ViewFactory<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  T,
> = (
  query: Query<TSchema, TTable, TReturn>,
  input: Input,
  format: Format,
  onDestroy: () => void,
  onTransactionCommit: (cb: () => void) => void,
  queryComplete: true | Promise<true>,
) => T;
