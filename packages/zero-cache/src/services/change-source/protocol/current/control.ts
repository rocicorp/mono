/**
 * Control plane messages communicate non-content related signals between a
 * ChangeSource and ChangeStreamer. These are not forwarded to subscribers
 * of the ChangeStreamer.
 */
import * as v from '../../../../../../shared/src/valita.js';

/**
 * Indicates that upstream has requested an acknowledgment from the ChangeStreamer,
 * often referred to as a heartbeat or keepalive message. The ChangeStreamer responds
 * to these messages immediately.
 */
export const ackRequestedSchema = v.object({
  tag: v.literal('ack-requested'),

  /**
   * If specified, indicates the latest watermark on upstream, which may encompass
   * changes that are unrelated to the ChangeStreamer's subscription. If the
   * ChangeStreamer has consumed and acked all of its data messages, it will include
   * this watermark in its acknowledgment to indicate that it is caught up and
   * upstream can purge its logs up to that watermark. If, on the other hand, it is
   * still processing data messages, it will acknowledge immediately without a
   * watermark to indicate that it is still "alive" but upstream logs still need to
   * be preserved.
   */
  latestUpstreamWatermark: v.string().optional(),
});

/**
 * Indicates that replication cannot continue and that the replica must be resynced
 * from scratch. The replication-manager will shutdown in response to this message,
 * and upon being restarted, it will wipe the current replica and resync if the
 * `--auto-reset` option is specified.
 *
 * This signal should only be used in well advertised scenarios, and is not suitable
 * as a common occurrence in production.
 */
export const resetRequiredSchema = v.object({tag: v.literal('reset-required')});
