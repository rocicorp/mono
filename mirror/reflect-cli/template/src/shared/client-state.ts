import {ReadTransaction, type WriteTransaction} from '@rocicorp/reflect/server';

const colors = ['#f94144', '#f3722c', '#f8961e', '#f9844a', '#f9c74f'];
const avatars = [
  ['🐶', 'Puppy'],
  ['🐱', 'Kitty'],
  ['🐭', 'Mouse'],
  ['🐹', 'Hamster'],
  ['🐰', 'Bunny'],
];

function randInt(min: number, max: number) {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min + 1) + min); //The maximum is inclusive and the minimum is inclusive
}

export function randUserInfo(): UserInfo {
  const [avatar, name] = avatars[randInt(0, avatars.length - 1)];
  return {
    avatar,
    name,
    color: colors[randInt(0, colors.length - 1)],
  };
}

export const clientStatePrefix = `client-state/`;

export function key(id: string): string {
  return `${clientStatePrefix}${id}`;
}

export type UserInfo = {
  name: string;
  avatar: string;
  color: string;
};

export type ClientState = {
  cursor?: {
    x: number;
    y: number;
  };
  userInfo: UserInfo;
};
export async function getClientState(
  tx: ReadTransaction,
  id: string,
): Promise<ClientState> {
  const jv = await tx.get(key(id));
  if (!jv) {
    throw new Error('Expected clientState to be initialized already: ' + id);
  }
  return jv as ClientState;
}

export function putClientState(
  tx: WriteTransaction,
  {clientState}: {clientState: ClientState},
): Promise<void> {
  return tx.put(key(tx.clientID), clientState);
}

export async function clearCursor(tx: WriteTransaction): Promise<void> {
  const clientState = {...(await getClientState(tx, tx.clientID))};
  clientState.cursor = undefined;
  await putClientState(tx, {
    clientState,
  });
}

export async function initClientState(
  tx: WriteTransaction,
  userInfo: UserInfo,
): Promise<void> {
  if (await tx.has(key(tx.clientID))) {
    return;
  }
  await putClientState(tx, {
    clientState: {
      userInfo,
    },
  });
}
