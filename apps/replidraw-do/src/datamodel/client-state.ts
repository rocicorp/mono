import type { ReadTransaction, WriteTransaction } from "@rocicorp/reflect";
import { randInt } from "../util/rand";

const colors = [
  "#f94144",
  "#f3722c",
  "#f8961e",
  "#f9844a",
  "#f9c74f",
  "#90be6d",
  "#43aa8b",
  "#4d908e",
  "#577590",
  "#277da1",
];
const avatars = [
  ["🐶", "Puppy"],
  ["🐱", "Kitty"],
  ["🐭", "Mouse"],
  ["🐹", "Hamster"],
  ["🐰", "Bunny"],
  ["🦊", "Fox"],
  ["🐻", "Bear"],
  ["🐼", "Panda"],
  ["🐻‍❄️", "Polar Bear"],
  ["🐨", "Koala"],
  ["🐯", "Tiger"],
  ["🦁", "Lion"],
  ["🐮", "Cow"],
  ["🐷", "Piggy"],
  ["🐵", "Monkey"],
  ["🐣", "Chick"],
];

import { z } from "zod";

export const userInfoSchema = z.object({
  avatar: z.string(),
  name: z.string(),
  color: z.string(),
});

// TODO: It would be good to merge this with the first-class concept of `client`
// that Replicache itself manages if possible.
export const clientStateSchema = z.object({
  cursor: z
    .object({
      x: z.number(),
      y: z.number(),
    })
    .optional(),
  overID: z.string(),
  selectedID: z.string(),
  userInfo: userInfoSchema,
});

export type UserInfo = z.infer<typeof userInfoSchema>;
export type ClientState = z.infer<typeof clientStateSchema>;

export async function initClientState(
  tx: WriteTransaction,
  { id, defaultUserInfo }: { id: string; defaultUserInfo: UserInfo }
): Promise<void> {
  if (await tx.has(key(id))) {
    return;
  }
  await putClientState(tx, {
    id,
    clientState: {
      overID: "",
      selectedID: "",
      userInfo: defaultUserInfo,
    },
  });
}

export async function getClientState(
  tx: ReadTransaction,
  id: string
): Promise<ClientState> {
  const jv = await tx.get(key(id));
  if (!jv) {
    throw new Error("Expected clientState to be initialized already: " + id);
  }
  return jv as ClientState;
  //return clientStateSchema.parse(jv);
}

export function putClientState(
  tx: WriteTransaction,
  { id, clientState }: { id: string; clientState: ClientState }
): Promise<void> {
  return tx.put(key(id), clientState);
}

export async function setCursor(
  tx: WriteTransaction,
  { id, x, y }: { id: string; x: number; y: number }
): Promise<void> {
  const clientState = await getClientState(tx, id);
  await putClientState(tx, {
    id,
    clientState: {
      ...clientState,
      cursor: {
        x,
        y,
      },
    },
  });
}

export async function clearCursorAndSelectionState(
  tx: WriteTransaction,
  { id }: { id: string }
): Promise<void> {
  const { cursor, ...clientState } = await getClientState(tx, id);
  await putClientState(tx, {
    id,
    clientState: {
      ...clientState,
      overID: "",
      selectedID: "",
    },
  });
}

export async function overShape(
  tx: WriteTransaction,
  { clientID, shapeID }: { clientID: string; shapeID: string }
): Promise<void> {
  const clientState = await getClientState(tx, clientID);
  await putClientState(tx, {
    id: clientID,
    clientState: {
      ...clientState,
      overID: shapeID,
    },
  });
}

export async function selectShape(
  tx: WriteTransaction,
  { clientID, shapeID }: { clientID: string; shapeID: string }
): Promise<void> {
  const clientState = await getClientState(tx, clientID);
  await putClientState(tx, {
    id: clientID,
    clientState: {
      ...clientState,
      selectedID: shapeID,
    },
  });
}

export function randUserInfo(): UserInfo {
  const [avatar, name] = avatars[randInt(0, avatars.length - 1)];
  return {
    avatar,
    name,
    color: colors[randInt(0, colors.length - 1)],
  };
}

function key(id: string): string {
  return `${clientStatePrefix}${id}`;
}

export const clientStatePrefix = `client-state-`;
