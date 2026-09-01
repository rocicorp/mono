import {h64} from '../../shared/src/hash.ts';
import {normalizeAST, type AST, type NormalizedAST} from './ast.ts';

const hashCache = new WeakMap<AST, string>();

export function hashOfAST(ast: AST): string {
  return hashOfNormalizedAST(normalizeAST(ast));
}

/**
 * The hash of an AST that is already normalized, e.g. the AST of a query
 * whose builder kept it normalized as it built it.
 */
export function hashOfNormalizedAST(ast: NormalizedAST): string {
  const cached = hashCache.get(ast);
  if (cached) {
    return cached;
  }
  const hash = h64(JSON.stringify(ast)).toString(36);
  hashCache.set(ast, hash);
  return hash;
}

export function hashOfNameAndArgs(
  name: string,
  args: readonly unknown[],
): string {
  const argsString = JSON.stringify(args);
  return h64(`${name}:${argsString}`).toString(36);
}
