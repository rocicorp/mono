import * as s from "superstruct";

export const connectedBodySchema = s.type({});

export const connectedMessageSchema = s.tuple([
  s.literal("connected"),
  connectedBodySchema,
]);

export type ConnectedBody = s.Infer<typeof connectedBodySchema>;
export type ConnectedMessage = s.Infer<typeof connectedMessageSchema>;
