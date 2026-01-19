import {unreachable} from '../../shared/src/asserts.ts';
import type {CRUDOp} from '../../zero-protocol/src/push.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {ServerTransaction} from '../../zql/src/mutate/custom.ts';

/**
 * Executes an array of CRUD operations against a server transaction.
 *
 * This function enables implementing CRUD mutators as custom mutators by
 * dynamically dispatching operations to the appropriate table mutator methods.
 *
 * @example
 * ```typescript
 * import {defineMutator, defineMutators} from '@rocicorp/zero';
 * import {executeCrudOps, type CRUDOp} from '@rocicorp/zero/server';
 *
 * export const mutators = defineMutators({
 *   crud: defineMutator(
 *     async ({tx, args}) => {
 *       if (tx.location !== 'server') {
 *         return; // Client-side is handled by optimistic CRUD
 *       }
 *       await executeCrudOps(tx, args.ops);
 *     },
 *   ),
 * });
 * ```
 */
export async function executeCrudOps<S extends Schema>(
  tx: ServerTransaction<S, unknown>,
  ops: CRUDOp[],
): Promise<void> {
  for (const op of ops) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const tableMutate = tx.mutate[op.tableName as keyof S['tables']] as any;
    switch (op.op) {
      case 'insert':
        await tableMutate.insert(op.value);
        break;
      case 'upsert':
        await tableMutate.upsert(op.value);
        break;
      case 'update':
        await tableMutate.update(op.value);
        break;
      case 'delete':
        await tableMutate.delete(op.value);
        break;
      default:
        unreachable(op);
    }
  }
}
