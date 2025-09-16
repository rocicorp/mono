/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
/**
 * Control plane messages communicate non-content related signals between a
 * ChangeSource and ChangeStreamer. These are not forwarded to subscribers
 * of the ChangeStreamer.
 */
import * as v from '../../../../../../shared/src/valita.ts';

/**
 * Indicates that replication cannot continue and that the replica must be resynced
 * from scratch. The replication-manager will shutdown in response to this message,
 * and upon being restarted, it will wipe the current replica and resync if the
 * `--auto-reset` option is specified.
 *
 * This signal should only be used in well advertised scenarios, and is not suitable
 * as a common occurrence in production.
 */
export const resetRequiredSchema = v.object({
  tag: v.literal('reset-required'),
  message: v.string().optional(),
});
