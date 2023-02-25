import * as s from 'superstruct';

export const invalidateForUserRequestSchema = s.type({userID: s.string()});
export const invalidateForRoomRequestSchema = s.type({roomID: s.string()});
export const connectionsResponseSchema = s.array(
  s.type({
    userID: s.string(),
    clientID: s.string(),
  }),
);

export type InvalidateForUserRequest = s.Infer<
  typeof invalidateForUserRequestSchema
>;
export type InvalidateForRoomRequest = s.Infer<
  typeof invalidateForRoomRequestSchema
>;
export type ConnectionsResponse = s.Infer<typeof connectionsResponseSchema>;
