import {h64} from '../../../shared/src/hash.ts';

/**
 * The value logged for a client group in place of its ID.
 *
 * A clientGroupID lets its holder join the group, so it does not belong in
 * logs. A hash still lets log lines about one group be correlated, which a
 * prefix would too but at the cost of revealing some of the ID's bits.
 */
export function clientGroupLogID(clientGroupID: string): string {
  return h64(clientGroupID).toString(36);
}
