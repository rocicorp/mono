import {version, type WriteTransaction} from '@rocicorp/reflect/server';
import {user} from './user.js';

export const mutators = {
  async init(tx: WriteTransaction) {
    await tx.put('init', {user});
    await tx.put('version', version);
  },

  async add(tx: WriteTransaction, data: {id: string; name: string}) {
    await tx.put(`users/${data.id}`, data.name);
  },

  async remove(tx: WriteTransaction, id: string) {
    await tx.del(`users/${id}`);
  },
};

export type Mutators = typeof mutators;
