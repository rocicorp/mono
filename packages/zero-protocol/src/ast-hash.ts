import {h64} from '../../shared/src/xxhash.js';
import {normalizeAST, type AST} from './ast.js';

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
