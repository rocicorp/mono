import {h64} from '../../shared/src/hash.ts';
import {normalizeAST, type AST} from './ast.ts';

const hashCache = new WeakMap<AST, string>();

export function hashOfAST(ast: AST): string {
  const normalized = normalizeAST(ast);
  const cached = hashCache.get(normalized);
  if (cached) {
    return cached;
  }
  const hash = h64(JSON.stringify(normalized)).toString(36);
  hashCache.set(normalized, hash);
  return hash;
}

export function hashOfNameAndArgs(
  name: string,
  args: readonly unknown[],
): string {
  const argsString = JSON.stringify(args);
  return h64(`${name}:${argsString}`).toString(36);
}
