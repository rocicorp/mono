import {unreachable} from '../../../shared/src/asserts.ts';
import type {Condition} from '../../../zero-protocol/src/ast.ts';

/**
 * Checks if a condition contains any NOT EXISTS operations.
 *
 * The client-side query engine cannot support NOT EXISTS operations because:
 *
 * 1. Zero only syncs a subset of data to the client, defined by the queries you use
 * 2. On the client, we can't distinguish between a row not existing at all vs.
 *    a row not being synced to the client
 * 3. For NOT EXISTS to work correctly, we would need complete knowledge of what
 *    doesn't exist, which is not reasonable with the partial sync model
 *
 * @param condition The condition to check
 * @throws Error if the condition uses NOT EXISTS operator
 */
export function assertNoNotExists(condition: Condition): void {
  switch (condition.type) {
    case 'simple':
      // Simple conditions don't use EXISTS/NOT EXISTS
      return;

    case 'correlatedSubquery':
      if (condition.op === 'NOT EXISTS') {
        throw new Error(
          'not(exists()) is not supported on the client - see https://bugs.rocicorp.dev/issue/3438',
        );
      }
      // Check if the subquery has a where condition
      if (condition.related.subquery.where) {
        assertNoNotExists(condition.related.subquery.where);
      }
      return;

    case 'and':
    case 'or':
      for (const c of condition.conditions) {
        assertNoNotExists(c);
      }
      return;
    default:
      unreachable(condition);
  }
}
