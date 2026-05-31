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

export const replicationTimingsSchema = changeSourceTimingsSchema.extend({
  replicateTimeMs: v.number(),
});

export const replicationReportSchema = v.object({
  lastTimings: replicationTimingsSchema.optional(),
  nextSendTimeMs: v.number(),
});

export type ChangeSourceTimings = v.Infer<typeof changeSourceTimingsSchema>;
export type ChangeSourceReport = v.Infer<typeof changeSourceReportSchema>;

export type ReplicationTimings = v.Infer<typeof replicationTimingsSchema>;
export type ReplicationReport = v.Infer<typeof replicationReportSchema>;
