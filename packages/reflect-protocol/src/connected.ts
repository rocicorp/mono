import * as v from '@badrap/valita';

export const connectedBodySchema = v.object({
  wsid: v.string(),
});

export const connectedMessageSchema = v.tuple([
  v.literal('connected'),
  connectedBodySchema,
]);

export type ConnectedBody = v.Infer<typeof connectedBodySchema>;
export type ConnectedMessage = v.Infer<typeof connectedMessageSchema>;
