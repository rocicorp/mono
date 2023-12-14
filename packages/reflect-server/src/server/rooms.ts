import type {LogContext} from '@rocicorp/logger';
import type {CreateRoomRequest} from 'reflect-protocol';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import * as valita from 'shared/src/valita.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import type {ListOptions} from '../storage/storage.js';
import {roomDOFetch} from './auth-do.js';
import {CREATE_ROOM_PATH, fmtPath} from './paths.js';

export enum RoomStatus {
  // An Open room can be used by users. We will accept connect()s to it.
  Open = 'open',
  // A Closed room cannot be used by users. We will reject connect()s to it.
  // Once closed, a room cannot be opened again.
  Closed = 'closed',
  // A Deleted room is a Closed room that has had all its data deleted.
  Deleted = 'deleted',
}

// The DurableStorage interface adds type-awareness to the DO Storage API. It
// requires a valita schema for values, which we define here. I've chosen
// the slightly non-DRY path of having a separate ts type definition and schema,
// instead of inferring the type from a schema, because frankly I like reading
// type definitions in the type definition language (ts) and want to keep goop
// (valita) from polluting the main ideas.
export const roomStatusSchema = valita.union(
  valita.literal(RoomStatus.Open),
  valita.literal(RoomStatus.Closed),
  valita.literal(RoomStatus.Deleted),
);

const jurisdictionSchema = valita.union(
  valita.literal(''),
  valita.literal('eu'),
);

const roomRecordSchema = valita.object({
  // roomID is the name of the room. It is externally visible, e.g.,
  // present in URLs.
  roomID: valita.string(),

  // objectIDString is the stringified Durable Object ID, the unique
  // identifier of the Durable Object instance that has this room.
  // It is not externally visible.
  //
  // In the past we derived the objectID from the roomID via idFromName, so
  // we didn't need to store the objectID. However in order to specify that
  // a DO should exist only in the EU for GDPR, we have to create objectIDs
  // via newUniqueId(). The unique ID is not derived from the roomID, so we
  // need to keep track of them eg when we receive connect() we need to
  // look the objectID up by roomID.
  objectIDString: valita.string(),

  // Indicates whether the room is pinned in the EU.
  jurisdiction: jurisdictionSchema,

  status: roomStatusSchema,
});

// RoomRecord keeps information about the room, for example the Durable
// Object ID of the DO instance that has the room.
export type RoomRecord = valita.Infer<typeof roomRecordSchema>;

// Public subset of RoomRecord that can be listed via the REST API
// (i.e. everything but the objectIDString).
//
// The schema is constructed by chaining the `roomRecordSchema` (as opposed to
// using `roomRecordSchema.pick(...)`) so that it can be passed into
// storage.listEntries(...) to validate the returned data with the
// full schema and from there construct the desired view.
export const roomPropertiesSchema = roomRecordSchema.chain(record =>
  valita.ok({
    roomID: record.roomID,
    jurisdiction: record.jurisdiction,
    // objectIDString is excluded since it's an internal CF detail.
    status: record.status,
  }),
);

export type RoomProperties = valita.Infer<typeof roomPropertiesSchema>;

export function internalCreateRoom(
  lc: LogContext,
  roomDO: DurableObjectNamespace,
  storage: DurableStorage,
  roomID: string,
  jurisdiction: 'eu' | undefined,
) {
  const url = `https://unused-reflect-room-do.dev${fmtPath(CREATE_ROOM_PATH, {
    roomID,
  })}`;
  const req: CreateRoomRequest = {jurisdiction};
  const request = new Request(url, {
    method: 'POST',
    // no auth headers, because this is an internal call
    body: JSON.stringify(req),
  });
  return createRoom(lc, roomDO, storage, request, roomID, jurisdiction);
}

// Note: caller must enforce no other concurrent calls to this and other
// functions that create or modify the room record.
export async function createRoom(
  lc: LogContext,
  roomDO: DurableObjectNamespace,
  storage: DurableStorage,
  request: Request,
  roomID: string,
  jurisdiction: 'eu' | undefined,
): Promise<Response> {
  // Note: this call was authenticated by dispatch, so no need to check for
  // authApiKey here.

  const invalidResponse = validateRoomID(roomID);
  if (invalidResponse) {
    return invalidResponse;
  }

  // Check if the room already exists.
  if ((await roomRecordByRoomID(storage, roomID)) !== undefined) {
    return new Response('room already exists', {
      status: 409 /* Conflict */,
    });
  }

  const options = jurisdiction ? {jurisdiction} : undefined;

  // Instantiate it so it will be listed in the namespace by the CF API,
  // and also so that it can do whatever it needs to initialize itself.
  const objectID = roomDO.newUniqueId(options);
  const newRoomDOStub = roomDO.get(objectID);
  const response = await roomDOFetch(
    request,
    'createRoom',
    newRoomDOStub,
    roomID,
    lc,
  );
  if (!response.ok) {
    lc.debug?.(
      `Received error response from ${roomID}. ${
        response.status
      } ${await response.clone().text()}`,
    );
    return response;
  }

  // Write the record for the room only after it has been successfully
  // instantiated and initialized.
  const roomRecord: RoomRecord = {
    roomID,
    objectIDString: objectID.toString(),
    jurisdiction: jurisdiction ?? '',
    status: RoomStatus.Open,
  };
  const roomRecordKey = roomKeyToString(roomRecord);
  await storage.put(roomRecordKey, roomRecord);
  lc.debug?.(`created room ${JSON.stringify(roomRecord)}`);

  return new Response('ok');
}

// Caller must enforce no other concurrent calls to this and other
// functions that create or modify the room record.
//
// Note that closeRoom closes the room but does NOT log users out of it.
// The call to closeRoom should be followed by a call to authInvalidateForRoom.
export async function closeRoom(
  lc: LogContext,
  storage: DurableStorage,
  roomID: string,
): Promise<Response> {
  const roomRecord = await roomRecordByRoomID(storage, roomID);
  if (roomRecord === undefined) {
    return new Response('no such room', {
      status: 404,
    });
  }

  if (roomRecord.status === RoomStatus.Closed) {
    return new Response('ok (room already closed)');
  } else if (roomRecord.status !== RoomStatus.Open) {
    return new Response('room is not open', {
      status: 409 /* Conflict */,
    });
  }

  roomRecord.status = RoomStatus.Closed;
  const roomRecordKey = roomKeyToString(roomRecord);
  await storage.put(roomRecordKey, roomRecord);
  lc.debug?.(`closed room ${JSON.stringify(roomRecord)}`);

  return new Response('ok');
}

// Caller must enforce no other concurrent calls to this and other
// functions that create or modify the room record.
export async function deleteRoom(
  lc: LogContext,
  roomDO: DurableObjectNamespace,
  storage: DurableStorage,
  roomID: string,
  request: Request,
): Promise<Response> {
  const roomRecord = await roomRecordByRoomID(storage, roomID);
  if (roomRecord === undefined) {
    return new Response('no such room', {
      status: 404,
    });
  }

  if (roomRecord.status === RoomStatus.Deleted) {
    return new Response('ok (room already deleted)');
  } else if (roomRecord.status !== RoomStatus.Closed) {
    return new Response('room must first be closed', {
      status: 409 /* Conflict */,
    });
  }

  const objectID = roomDO.idFromString(roomRecord.objectIDString);
  const roomDOStub = roomDO.get(objectID);
  const response = await roomDOFetch(
    request,
    'deleteRoom',
    roomDOStub,
    roomID,
    lc,
  );
  if (!response.ok) {
    lc.debug?.(
      `Received error response from ${roomID}. ${
        response.status
      } ${await response.clone().text()}`,
    );
    return response;
  }

  roomRecord.status = RoomStatus.Deleted;
  const roomRecordKey = roomKeyToString(roomRecord);
  await storage.put(roomRecordKey, roomRecord);
  lc.debug?.(`deleted room ${JSON.stringify(roomRecord)}`);
  return new Response('ok');
}

const roomIDRegex = /^[A-Za-z0-9_\-/]+$/;

function validateRoomID(roomID: string) {
  if (!roomIDRegex.test(roomID)) {
    return new Response(`Invalid roomID (must match ${roomIDRegex})`, {
      status: 400,
    });
  }
  return undefined;
}

// Caller must enforce no other concurrent calls to
// functions that create or modify the room record.
export async function objectIDByRoomID(
  storage: DurableStorage,
  roomDO: DurableObjectNamespace,
  roomID: string,
) {
  const roomRecord = await roomRecordByRoomID(storage, roomID);
  if (roomRecord === undefined) {
    return undefined;
  }
  return roomDO.idFromString(roomRecord.objectIDString);
}

// Caller must enforce no other concurrent calls to
// functions that create or modify the room record.
export function roomRecordByRoomID(storage: DurableStorage, roomID: string) {
  return roomDataByRoomID(storage, roomID, roomRecordSchema);
}

// Caller must enforce no other concurrent calls to
// functions that create or modify the room record.
export function roomPropertiesByRoomID(
  storage: DurableStorage,
  roomID: string,
) {
  return roomDataByRoomID(storage, roomID, roomPropertiesSchema);
}

function roomDataByRoomID<T extends ReadonlyJSONValue>(
  storage: DurableStorage,
  roomID: string,
  schema: valita.Type<T>,
) {
  const roomRecordKey = roomKeyToString({roomID});
  return storage.get(roomRecordKey, schema);
}

export async function roomRecordByObjectIDForTest(
  storage: DurableStorage,
  objectID: DurableObjectId,
) {
  // Sure, inefficient, but it works just fine for now.
  const roomRecords = await storage.list(
    {prefix: ROOM_KEY_PREFIX},
    roomRecordSchema,
  );
  const needle = objectID.toString();
  for (const roomRecord of roomRecords.values()) {
    if (roomRecord.objectIDString === needle) {
      return roomRecord;
    }
  }
  return undefined;
}

export async function roomProperties(
  storage: DurableStorage,
  opts: ListOptions,
) {
  const options = convertListOptionKeysToRoomKeys(opts);
  const map = await storage.list(options, roomPropertiesSchema);
  return map.values();
}

// Storage key types are intentionally not exported so that other modules
// don't know too much about the innards of the storage. They should use
// the exported functions to access the storage.
const ROOM_KEY_PREFIX = 'room/';

type RoomKey = {
  roomID: string;
};

function roomKeyToString(key: RoomKey): string {
  return `${ROOM_KEY_PREFIX}${encodeURIComponent(key.roomID)}/`;
}

export function convertListOptionKeysToRoomKeys(
  opts: ListOptions,
): ListOptions {
  const prefix = ROOM_KEY_PREFIX + (opts.prefix ?? '');
  const options = {...opts, prefix};
  if (opts.start) {
    options.start = {
      ...opts.start,
      // Encoding the empty key with roomKeyToString() would result in the start key being "room//",
      // which is lexicographically larger than "room/-.*/". Instead, leave the empty string as-is
      // so that it preserves the semantics of "before the first valid key".
      key:
        opts.start.key === '' ? '' : roomKeyToString({roomID: opts.start.key}),
    };
  }
  return options;
}
