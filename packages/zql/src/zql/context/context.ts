import type {AST} from '../ast2/ast.js';

export type GotCallback = (got: boolean) => void;

export type SubscriptionDelegate = {
  subscriptionAdded(ast: AST, gotCallback?: GotCallback): () => void;
};
