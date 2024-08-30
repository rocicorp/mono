import type {AST} from '../ast/ast.js';

export type SubscriptionDelegate = {
  subscriptionAdded(ast: AST): () => void;
};
