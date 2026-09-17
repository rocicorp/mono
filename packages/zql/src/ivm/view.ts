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
 * A view notifies its observers from the callback it registers with
 * `onTransactionCommit`, not from `push`. Usually the pushes of a transaction
 * and its commit happen in one synchronous task, so nothing can look at the
 * view in between. The exception is views materialized before the client's
 * data is loaded: they are hydrated in time slices, over several tasks, and
 * committed together. A view whose state is readable outside of those
 * notifications (as `ArrayView.data` is) would show its rows ahead of the
 * views it is committed with. Such a view can implement the optional
 * `holdData()`, called right before that hydration, to keep showing its
 * current snapshot, and `releaseData()`, called on every view of the batch
 * right before they are all committed, to stop.
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
