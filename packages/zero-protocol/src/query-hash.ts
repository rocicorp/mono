import {normalizeAST, type AST} from './ast.ts';
import {hashAST, hashNameAndArgs} from './query-hash-visitor.ts';

const hashCache = new WeakMap<AST, string>();

export function hashOfAST(ast: AST): string {
  const normalized = normalizeAST(ast);
  const cached = hashCache.get(normalized);
  if (cached) {
    return cached;
  }
  const hash = hashAST(normalized);
  hashCache.set(normalized, hash);
  return hash;
}

export function hashOfNameAndArgs(
  name: string,
  args: readonly unknown[],
): string {
  return hashNameAndArgs(name, args);
}
