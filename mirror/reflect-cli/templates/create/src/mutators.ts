// This file defines our "mutators".
//
// Mutators are how you change data in Reflect apps.
//
// They are registered with Reflect at construction-time and callable like:
// `myReflect.mutate.setCursor()`.
//
// Reflect runs each mutation immediately (optimistically) on the client,
// against the local cache, and then later (usually moments later) sends a
// description of the mutation (its name and arguments) to the server, so that
// the server can *re-run* the mutation there against the authoritative
// datastore.
//
// This re-running of mutations is how Reflect handles conflicts: the
// mutators defensively check the database when they run and do the appropriate
// thing. The Reflect sync protocol ensures that the server-side result takes
// precedence over the client-side optimistic result.

import type {WriteTransaction} from '@rocicorp/reflect';
import {initClientState, updateClientState} from './client-state.js';

export const mutators = {
  setCursor,
  initClientState,
  increment,
};

export type M = typeof mutators;

async function increment(
  tx: WriteTransaction,
  {key, delta}: {key: string; delta: number},
) {
  console.log(`incrementing ${key} by ${delta}`);
  const prev = await tx.get<number>(key);
  const next = (prev ?? 0) + delta;
  await tx.set(key, next);
}

async function setCursor(
  tx: WriteTransaction,
  {x, y}: {x: number; y: number},
): Promise<void> {
  await updateClientState(tx, {id: tx.clientID, cursor: {x, y}});
}
