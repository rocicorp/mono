import {entitySchema, generate} from '@rocicorp/rails';
import type {ReadTransaction, WriteTransaction} from '@rocicorp/reflect/client';
import {z} from 'zod';

// Changing this string to the next in the sequence a-z,aa-zz,aaa-zzz,..
// will force all new orchestrator and puzzle rooms.  This
// can be useful if we make breaking schema changes or simply want
// rooms with less garbage built up (which can slow down initial sync).
const ROOMS_VERSION = 'b';

export const ORCHESTRATOR_ROOM = `orch-${ROOMS_VERSION}`;

const MAX_CLIENTS_PER_ROOM = 15;
const CLIENT_ROOM_ASSIGNMENT_GC_THRESHOLD_MS = 3 * 60_000;
const CLIENT_ROOM_ASSIGNMENT_GC_INTERVAL_MS = 10_000;

const roomIndexToRoomID = (index: number) =>
  `puzzle-${ROOMS_VERSION}-${index.toString(10).padStart(7, '0')}`;

const roomModelSchema = entitySchema.extend({
  clientCount: z.number(),
});

export const clientRoomAssignmentSchema = entitySchema.extend({
  // foreign key to a roomModelSchema
  roomID: z.string(),
  aliveTimestamp: z.number(),
});

const clientRoomAssignmentMetaSchema = z.object({
  lastGCTimestamp: z.number(),
});

// Export generated interface.
export type ClientRoomAssignmentModel = z.infer<
  typeof clientRoomAssignmentSchema
>;
type ClientRoomAssignmentMetaModel = z.infer<
  typeof clientRoomAssignmentMetaSchema
>;

const {get: getRoom, put: putRoom} = generate('room', roomModelSchema);

const {
  get: getClientRoomAssignmentInternal,
  put: putClientRoomAssignment,
  update: updateClientRoomAssignment,
  delete: deleteClientRoomAssignment,
  list: listClientRoomAssignments,
} = generate('clientToRoom', clientRoomAssignmentSchema);

export const getClientRoomAssignment = getClientRoomAssignmentInternal;

const clientRoomAssignmentMetaKey = 'clientToRoomMeta';
async function getClientRoomAssignmentMeta(
  tx: ReadTransaction,
): Promise<ClientRoomAssignmentMetaModel | undefined> {
  const meta = await tx.get(clientRoomAssignmentMetaKey);
  if (meta === undefined) {
    return meta;
  }
  return clientRoomAssignmentMetaSchema.parse(meta);
}

async function putClientRoomAssignmentMeta(
  tx: WriteTransaction,
  meta: ClientRoomAssignmentMetaModel,
) {
  await tx.put(clientRoomAssignmentMetaKey, meta);
}

async function updateRoomClientCount(
  tx: WriteTransaction,
  roomID: string,
  change: number,
) {
  const room = await getRoom(tx, roomID);
  if (room !== undefined) {
    await putRoom(tx, {
      id: room.id,
      clientCount: Math.max(room.clientCount + change, 0),
    });
  }
}

// Mutators
export async function alive(tx: WriteTransaction) {
  if (tx.environment !== 'server') {
    return;
  }
  const now = Date.now();
  const clientRoomAssignmentMeta = await getClientRoomAssignmentMeta(tx);
  if (
    clientRoomAssignmentMeta === undefined ||
    now - clientRoomAssignmentMeta.lastGCTimestamp >
      CLIENT_ROOM_ASSIGNMENT_GC_INTERVAL_MS
  ) {
    await putClientRoomAssignmentMeta(tx, {lastGCTimestamp: now});
    // GC room assignments
    const assignments = await listClientRoomAssignments(tx);
    const toDelete = [];
    const roomCountChanges = new Map();
    for (const assignment of assignments) {
      if (
        now - assignment.aliveTimestamp >
        CLIENT_ROOM_ASSIGNMENT_GC_THRESHOLD_MS
      ) {
        toDelete.push(assignment);
        roomCountChanges.set(
          assignment.roomID,
          (roomCountChanges.get(assignment.roomID) ?? 0) - 1,
        );
      }
    }
    console.log('GCing', toDelete.length, 'assignments of', assignments.length);
    await Promise.all(
      toDelete.map(assignment => deleteClientRoomAssignment(tx, assignment.id)),
    );
    await Promise.all(
      [...roomCountChanges.entries()].map(async ([roomID, change]) => {
        await updateRoomClientCount(tx, roomID, change);
      }),
    );
  }
  const clientRoomAssignment = await getClientRoomAssignment(tx, tx.clientID);
  if (clientRoomAssignment !== undefined) {
    await updateClientRoomAssignment(tx, {
      id: clientRoomAssignment.id,
      aliveTimestamp: now,
    });
    return;
  }
  // assign a room
  for (let roomIndex = 0, roomAssigned = false; !roomAssigned; roomIndex++) {
    const roomID = roomIndexToRoomID(roomIndex);
    const room = await getRoom(tx, roomID);
    const clientCount = room?.clientCount ?? 0;
    if (clientCount < MAX_CLIENTS_PER_ROOM) {
      if (room === undefined) {
        await putRoom(tx, {
          id: roomID,
          clientCount: 1,
        });
      } else {
        await updateRoomClientCount(tx, roomID, 1);
      }
      await putClientRoomAssignment(tx, {
        id: tx.clientID,
        roomID,
        aliveTimestamp: now,
      });
      roomAssigned = true;
      console.log('assigned', tx.clientID, 'to room', JSON.stringify(room));
    }
  }
}

export async function unload(tx: WriteTransaction) {
  if (tx.environment !== 'server') {
    return;
  }
  const assignment = await getClientRoomAssignment(tx, tx.clientID);
  if (assignment !== undefined) {
    console.log(
      'removing assignment due to unload',
      JSON.stringify(assignment),
    );
    await Promise.all([
      await updateRoomClientCount(tx, assignment.roomID, -1),
      await deleteClientRoomAssignment(tx, assignment.id),
    ]);
  }
}
