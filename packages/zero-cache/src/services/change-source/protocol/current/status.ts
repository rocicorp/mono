/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import * as v from '../../../../../../shared/src/valita.ts';

/**
 * The StatusMessage payload itself is unspecified. The `zero-cache` will
 * send the Commit payload when acknowledging a completed transaction, and
 * will echo back whatever message was sent from the ChangeSource when
 * acknowledging a downstream StatusMessage.
 */
export const statusSchema = v.object({});

export const statusMessageSchema = v.tuple([
  v.literal('status'),
  statusSchema,
  v.object({watermark: v.string()}),
]);

/**
 * A StatusMessage conveys positional information from both the ChangeSource
 * and the `zero-cache`.
 *
 * A StatusMessage from the ChangeSource indicates a position in its change
 * log. Generally, the watermarks sent in `Commit` messages already convey
 * this information, but a StatusMessage may also be sent to indicate that the
 * log has progressed without any corresponding changes relevant to the
 * subscriber. The watermarks of commit messages and status messages must be
 * monotonic in the stream of messages from the ChangeSource.
 *
 * The `zero-cache` sends StatusMessages to the ChangeSource:
 *
 * * when it has processed a `Commit` received from the ChangeSource
 *
 * * when it receives a `StatusMessage` and all preceding `Commit` messages
 *   have been processed
 *
 * This allows the ChangeSource to clean up change log entries appropriately.
 *
 * Note that StatusMessages from the ChangeSource are optional. If a
 * ChangeSource implementation can track subscriber progress and clean up
 * its change log purely from Commit-driven StatusMessages there is no need
 * for the ChangeSource to send StatusMessages.
 */
export type StatusMessage = v.Infer<typeof statusMessageSchema>;
