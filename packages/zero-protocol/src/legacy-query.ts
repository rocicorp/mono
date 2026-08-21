import * as v from '../../shared/src/valita.ts';
import type {AST} from './ast.ts';

/**
 * Rejects a client-supplied legacy query without inspecting its AST.
 *
 * Keep the AST output type so clients built with the legacy API continue to
 * type-check and receive a protocol error from zero-cache at runtime.
 */
export const unsupportedLegacyQueryASTSchema = v
  .unknown()
  .chain<AST>(() => v.err({message: 'Legacy queries are not supported'}));
