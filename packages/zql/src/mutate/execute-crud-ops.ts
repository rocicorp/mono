import {unreachable} from '../../../shared/src/asserts.ts';
import type {CRUDOp} from '../../../zero-protocol/src/push.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {TransactionBase} from './custom.ts';

/**
 * Executes an array of CRUD operations against a transaction.
 *
 * This function enables implementing CRUD mutators as custom mutators by
 * dynamically dispatching operations to the appropriate table mutator methods.
 * Works with both ClientTransaction (optimistic) and ServerTransaction (authoritative).
 *
 * @example
 * ```typescript
 * import {defineMutator, defineMutators, executeCrudOps} from '@rocicorp/zero';
 * import type {CRUDOp} from '@rocicorp/zero';
 *
 * export const mutators = defineMutators({
 *   crud: defineMutator(
 *     async ({tx, args}: {tx: Transaction<Schema>; args: {ops: CRUDOp[]}}) => {
 *       // Works on both client (optimistic) and server (authoritative)
 *       await executeCrudOps(tx, args.ops);
 *     },
 *   ),
 * });
 * ```
 */
export async function executeCrudOps<S extends Schema>(
  tx: TransactionBase<S>,
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
