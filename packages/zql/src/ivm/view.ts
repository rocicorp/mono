import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import type {Value} from '../../../zero-protocol/src/data.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {Query} from '../query/query.ts';
import type {TTL} from '../query/ttl.ts';
import type {Input} from './operator.ts';

export type View = EntryList | Entry | undefined;
export type EntryList = readonly Entry[];
export type Entry = {readonly [key: string]: Value | View};

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
  queryComplete: true | ErroredQuery | Promise<true>,
  updateTTL: (ttl: TTL) => void,
) => T;
