import * as v from '../../../../../../shared/src/valita.js';

/**
 * An acknowledgement is sent from the Change Streamer to the Change Source to
 * either or both:
 * * Indicate that it is connected and running, e.g. in response to an
 *   "ack-requested" message.
 * * Indicate that it is caught up to a specific `watermark`.
 */
export const ackSchema = v.union(
  v.tuple([v.literal('ack')]),
  v.tuple([v.literal('ack'), v.object({watermark: v.string()})]),
);

/** At the moment, the only upstream messages are acks.  */
export const changeSourceUpstreamSchema = ackSchema;
export type ChangeSourceUpstream = v.Infer<typeof changeSourceUpstreamSchema>;
