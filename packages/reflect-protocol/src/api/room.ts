import * as v from 'shared/src/valita.js';

export const createRoomRequestSchema = v.object({
  jurisdiction: v.literal('eu').optional(),
});

export type CreateRoomRequest = v.Infer<typeof createRoomRequestSchema>;
