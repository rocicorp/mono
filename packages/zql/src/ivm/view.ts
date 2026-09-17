import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import type {Value} from '../../../zero-protocol/src/data.ts';
import type {Format} from '../../../zero-types/src/format.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {Query} from '../query/query.ts';
import type {TTL} from '../query/ttl.ts';
import type {Input} from './operator.ts';

export type View = EntryList | Entry | undefined;
export type EntryList = readonly Entry[];
export type Entry = {readonly [key: string]: Value | View};

export type {Format};

/**
 * Creates the view `materialize` returns.
 *
 * A view must not make pushed rows observable until the callback it
 * registered with `onTransactionCommit` is called. The pushes of one
 * transaction are not always delivered in a single task: views materialized
 * before the client's data is loaded are hydrated in time slices and committed
 * together, so a view that shows rows as they are pushed shows them ahead of
 * the views it is committed with. A view that does apply pushes directly can
 * implement `holdData()`, which is called right before such a hydration and
 * should keep the current snapshot visible until the commit.
 */
export type ViewFactory<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn,
  T,
> = (
  query: Query<TTable, TSchema, TReturn>,
  input: Input,
  format: Format,
  onDestroy: () => void,
  onTransactionCommit: (cb: () => void) => void,
  queryComplete: true | ErroredQuery | Promise<true>,
  updateTTL: (ttl: TTL) => void,
) => T;

// oxlint-disable-next-line no-explicit-any
export type AnyViewFactory = ViewFactory<string, Schema, any, any>;
