import * as v from '../../../../../shared/src/valita.ts';

export const changeSourceTimingsSchema = v.object({
  sendTimeMs: v.number(),
  commitTimeMs: v.number(),
  receiveTimeMs: v.number(),
});

export const changeSourceReportSchema = v.object({
  lastTimings: changeSourceTimingsSchema,
  nextSendTimeMs: v.number(),
});

const replicationTimingsSchema = changeSourceTimingsSchema.extend({
  // When the replicator received the report from the change-streamer. The
  // time from here to `replicateTimeMs` is spent in this process, queued
  // behind and applying the changes that precede the report. The time from
  // `receiveTimeMs` to here is spent in the change-streamer and the network
  // (and is subject to clock skew if they run on different hosts).
  deliverTimeMs: v.number().optional(),
  replicateTimeMs: v.number(),
});

const replicationReportSchema = v.object({
  lastTimings: replicationTimingsSchema.optional(),
  nextSendTimeMs: v.number(),
});

export type ReplicationReport = v.Infer<typeof replicationReportSchema>;
