import * as s from 'superstruct';

export const createRoomRequestSchema = s.type({
  roomID: s.string(),
  jurisdiction: s.optional(s.literal('eu')),
});

export type CreateRoomRequest = s.Infer<typeof createRoomRequestSchema>;
