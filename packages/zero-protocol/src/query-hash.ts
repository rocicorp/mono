import {getOrInsertComputed} from '../../shared/src/map.ts';
import {normalizeAST, type AST, type NormalizedAST} from './ast.ts';
import {hashAST} from './query-hash-visitor.ts';

const hashCache = new WeakMap<AST, string>();

export function hashOfAST(ast: AST): string {
  return hashOfNormalizedAST(normalizeAST(ast));
}

/**
 * The hash of an AST that is already normalized, e.g. the AST of a query
 * whose builder kept it normalized as it built it.
 */
export function hashOfNormalizedAST(ast: NormalizedAST): string {
  return getOrInsertComputed(hashCache, ast, hashAST);
}

export {hashNameAndArgs as hashOfNameAndArgs} from './query-hash-visitor.ts';
