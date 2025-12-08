import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import type {Value} from '../../../zero-protocol/src/data.ts';
import type {Format} from '../../../zero-types/src/format.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {QueryBuilder} from '../query/query-builder.ts';
import type {TTL} from '../query/ttl.ts';
import type {Input} from './operator.ts';

export type View = EntryList | Entry | undefined;
export type EntryList = readonly Entry[];
export type Entry = {readonly [key: string]: Value | View};

export type {Format};

export type ViewFactory<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn,
  T,
> = (
  query: QueryBuilder<TTable, TSchema, TReturn>,
  input: Input,
  format: Format,
  onDestroy: () => void,
  onTransactionCommit: (cb: () => void) => void,
  queryComplete: true | ErroredQuery | Promise<true>,
  updateTTL: (ttl: TTL) => void,
) => T;

// oxlint-disable-next-line no-explicit-any
export type AnyViewFactory = ViewFactory<string, Schema, any, any>;
