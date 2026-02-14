import * as v from '../../../../../../shared/src/valita.ts';
import {commitSchema} from './data.ts';

/**
 * The downstream status message indicates whether it should be echoed
 * back in an upstream status message.
 */
export const downstreamStatusSchema = v.object({
  ack: v.boolean().optional(() => true),
});

export type DownstreamStatus = v.Infer<typeof downstreamStatusSchema>;

export const downstreamStatusMessageSchema = v.tuple([
  v.literal('status'),
  downstreamStatusSchema,
  v.object({watermark: v.string()}),
]);

/**
 * The `zero-cache` will send the Commit payload when acknowledging a
 * completed transaction, and will echo back the downstreamStatus message
 * if `ack` is true.
 */
export const upstreamStatusMessageSchema = v.tuple([
  v.literal('status'),
  v.union(downstreamStatusSchema, commitSchema),
  v.object({watermark: v.string()}),
]);

/**
 * Status messages convey positional information from both the ChangeSource
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
export type DownstreamStatusMessage = v.Infer<
  typeof downstreamStatusMessageSchema
>;
export type UpstreamStatusMessage = v.Infer<typeof upstreamStatusMessageSchema>;
