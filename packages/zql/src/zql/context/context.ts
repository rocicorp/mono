import type {AST, Ordering} from '../ast/ast.js';
import type {Materialite} from '../ivm/materialite.js';
import type {Source} from '../ivm/source/source.js';
import type {PipelineEntity} from '../ivm/types.js';

export type SubscriptionDelegate = {
  subscriptionAdded(ast: AST): void;
  subscriptionRemoved(ast: AST): void;
};

/**
 * Used to integrate with the host environment.
 *
 * A source is a table or collection which ZQL can query.
 * The name of a source represents the name of the table
 * ZQL is querying.
 */
export type Context = SubscriptionDelegate & {
  materialite: Materialite;
  getSource: <T extends PipelineEntity>(
    name: string,
    ordering: Ordering | undefined,
  ) => Source<T>;
};
