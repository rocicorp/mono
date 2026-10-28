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
  replicateTimeMs: v.number(),
});

const replicationReportSchema = v.object({
  lastTimings: replicationTimingsSchema.optional(),
  nextSendTimeMs: v.number(),
});

export type ReplicationReport = v.Infer<typeof replicationReportSchema>;
