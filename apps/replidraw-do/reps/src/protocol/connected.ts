import { z } from "zod";

export const connectedBodySchema = z.object({});

export const connectedMessageSchema = z.tuple([
  z.literal("connected"),
  connectedBodySchema,
]);

export type ConnectedBody = z.infer<typeof connectedBodySchema>;
export type ConnectedMessage = z.infer<typeof connectedMessageSchema>;
