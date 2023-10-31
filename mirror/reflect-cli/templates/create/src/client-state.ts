// This file defines the ClientState entity that we use to track
// cursors. It also defines some basic CRUD functions using the
// @rocicorp/rails helper library.

import type {WriteTransaction} from '@rocicorp/reflect';
import {Entity, generate} from '@rocicorp/rails';

export type ClientState = Entity & {
  cursor: {x: number; y: number} | null;
  userInfo: UserInfo;
};

export type UserInfo = {
  name: string;
  avatar: string;
  color: string;
};

export {
  initClientState,
  getClientState,
  putClientState,
  updateClientState,
  randUserInfo,
};

const {
  init: initImpl,
  get: getClientState,
  put: putClientState,
  update: updateClientState,
} = generate<ClientState>('client-state');

function initClientState(tx: WriteTransaction, userInfo: UserInfo) {
  return initImpl(tx, {
    id: tx.clientID,
    cursor: null,
    userInfo,
  });
}

function randUserInfo(): UserInfo {
  const [avatar, name] = avatars[randInt(0, avatars.length - 1)];
  return {
    avatar,
    name,
    color: colors[randInt(0, colors.length - 1)],
  };
}

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
