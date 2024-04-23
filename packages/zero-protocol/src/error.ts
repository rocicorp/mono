import * as v from 'shared/src/valita.js';

// Note: Metric names depend on these values,
// so if you add or change on here a corresponding dashboard
// change will likely be needed.

export const errorKindSchema = v.union(
  // v.literal('AuthInvalidated'),
  v.literal('ClientNotFound'),
  v.literal('InvalidConnectionRequest'),
  v.literal('InvalidConnectionRequestBaseCookie'),
  v.literal('InvalidConnectionRequestLastMutationID'),
  v.literal('InvalidConnectionRequestClientDeleted'),
  v.literal('InvalidMessage'),
  //v.literal('InvalidPush'),
  //v.literal('RoomClosed'),
  //v.literal('RoomNotFound'),
  //v.literal('Unauthorized'),
  v.literal('VersionNotSupported'),
);

export type ErrorKind = v.Infer<typeof errorKindSchema>;

export const errorMessageSchema = v.tuple([
  v.literal('error'),
  errorKindSchema,
  v.string(),
]);

export type ErrorMessage = ['error', ErrorKind, string];
