import * as v from 'shared/src/valita.js';

// Note: Metric names depend on these values,
// so if you add or change on here a corresponding dashboard
// change will likely be needed.

export const errorKindSchema = v.union(
  v.literal('AuthInvalidated'),
  v.literal('ClientNotFound'),
  v.literal('InvalidConnectionRequest'),
  v.literal('InvalidConnectionRequestBaseCookie'),
  v.literal('InvalidConnectionRequestLastMutationID'),
  v.literal('InvalidConnectionRequestClientDeleted'),
  v.literal('InvalidMessage'),
  v.literal('InvalidPush'),
  // TODO: This error should include the ID of the mutation that failed so that
  // the app can update the UI if it wants. This requires restructuring the
  // protocol types a little since some error kinds will have additional info
  // and others will not.
  v.literal('MutationFailed'),
  v.literal('RoomClosed'),
  v.literal('RoomNotFound'),
  v.literal('Unauthorized'),
  v.literal('VersionNotSupported'),
);

export type ErrorKind = v.Infer<typeof errorKindSchema>;

export const errorMessageSchema = v.tuple([
  v.literal('error'),
  errorKindSchema,
  v.string(),
]);

export type ErrorMessage = ['error', ErrorKind, string];
