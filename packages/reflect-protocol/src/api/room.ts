import * as v from 'shared/src/valita.js';

export const createRoomRequestSchema = v.object({
  roomID: v.string(),
  jurisdiction: v.literal('eu').optional(),
});

export type CreateRoomRequest = v.Infer<typeof createRoomRequestSchema>;
