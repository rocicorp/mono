import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';

// Replace this with the `subQueries` field if support is added in the base AST type.
export type SubQuery = {
  readonly ast: ServerAST;
  readonly alias: string;
};

export type ServerAST = AST & {
  // At the moment, only a single subQuery is supported,
  // and it overrides the AST's `table`.
  subQuery?: SubQuery | undefined;
};
