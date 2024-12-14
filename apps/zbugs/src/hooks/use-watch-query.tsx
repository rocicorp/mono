import type {Query, QueryType, Row, TableSchema} from '@rocicorp/zero';
import {useEffect} from 'react';
import {unreachable} from 'shared/src/asserts.js';
import type {Change as IVMChange} from 'zql/src/ivm/change.js';
import type {Input, Output} from 'zql/src/ivm/operator.js';
import type {Format} from 'zql/src/ivm/view.js';
import type {AdvancedQuery} from 'zql/src/query/query-internal.js';

export function useWatchQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(
  q: Query<TSchema, TReturn>,
  onChange: (change: Change<TSchema>) => void,
  enabled = true,
): void {
  useEffect(() => {
    if (!enabled) {
      return;
    }
    return (q as AdvancedQuery<TSchema, TReturn>)
      .materialize(changeViewFactory)
      .subscribe(onChange);
  }, [q, onChange, enabled]);
}

class WatchQuery<TSchema extends TableSchema> implements Output {
  readonly #subscribers = new Set<Callback<TSchema>>();
  readonly #onDestroy: () => void;
  #queryComplete = false;
  #pendingChanges: IVMChange[] = [];

  constructor(
    input: Input,
    onDestroy: () => void = () => void 0,
    queryComplete: true | Promise<true>,
  ) {
    this.#onDestroy = onDestroy;
    if (queryComplete === true) {
      this.#queryComplete = true;
    } else {
      queryComplete.then(() => {
        this.#queryComplete = true;
        this.flush();
      });
    }
    input.setOutput(this);
  }

  destroy() {
    this.#onDestroy();
  }

  push(change: IVMChange): void {
    this.#pendingChanges.push(change);
  }

  subscribe(cb: Callback<TSchema>): () => void {
    this.#subscribers.add(cb);
    return () => {
      this.#subscribers.delete(cb);
    };
  }

  flush() {
    if (!this.#queryComplete) {
      return;
    }

    for (const change of this.#pendingChanges) {
      for (const cb of this.#subscribers) {
        cb(toChange(change));
      }
    }
    this.#pendingChanges = [];
  }
}

function changeViewFactory<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(
  _query: Query<TSchema, TReturn>,
  input: Input,
  _format: Format,
  onDestroy: () => void,
  onTransactionCommit: (cb: () => void) => void,
  queryComplete: true | Promise<true>,
): WatchQuery<TSchema> {
  const changeView = new WatchQuery(input, onDestroy, queryComplete);
  onTransactionCommit(() => {
    changeView.flush();
  });
  return changeView;
}

export type Change<TSchema extends TableSchema> =
  | {
      type: 'add';
      row: Row<TSchema>;
    }
  | {
      type: 'remove';
      row: Row<TSchema>;
    }
  | {
      type: 'child';
      row: Row<TSchema>;
      child: {
        relationshipName: string;
        // TODO: Find this from the relationship.
        change: Change<TableSchema>;
      };
    }
  | {
      type: 'edit';
      row: Row<TSchema>;
      oldRow: Row<TSchema>;
    };

export type Callback<TSchema extends TableSchema> = (
  changes: Change<TSchema>,
) => void;

function toChange<TSchema extends TableSchema>(
  change: IVMChange,
): Change<TSchema> {
  switch (change.type) {
    case 'add':
      return {type: 'add', row: change.node.row as Row<TSchema>};
    case 'remove':
      return {type: 'remove', row: change.node.row as Row<TSchema>};
    case 'child':
      return {
        type: 'child',
        row: change.row as Row<TSchema>,
        child: {
          relationshipName: change.child.relationshipName,
          change: toChange(change.child.change),
        },
      };
    case 'edit':
      return {
        type: 'edit',
        row: change.node.row as Row<TSchema>,
        oldRow: change.oldNode.row as Row<TSchema>,
      };
    default:
      unreachable(change);
  }
}
