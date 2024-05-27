import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';

export type AggSelect = {
  selectors: {
    column: string;
    alias: string;
  }[];
  table: string;
  alias: string;
};
export type ServerAST = AST & {
  aggLift?: AggSelect[] | undefined;
};
