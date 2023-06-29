import * as v from 'shared/src/valita.js';

export const invalidateForUserRequestSchema = v.object({
  userID: v.string(),
});
export const invalidateForRoomRequestSchema = v.object({
  roomID: v.string(),
});
export const connectionsResponseSchema = v.array(
  v.object({
    userID: v.string(),
    clientID: v.string(),
  }),
);

export type InvalidateForUserRequest = v.Infer<
  typeof invalidateForUserRequestSchema
>;
export type InvalidateForRoomRequest = v.Infer<
  typeof invalidateForRoomRequestSchema
>;
export type ConnectionsResponse = v.Infer<typeof connectionsResponseSchema>;
