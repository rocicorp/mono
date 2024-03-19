import type {Patch, Poke} from 'reflect-protocol';

export function mergePokes(toMerge: Poke[]): Poke | undefined {
  if (toMerge.length === 0) {
    return undefined;
  }
  const mergedPatch: Patch = [];
  const mergedPresence: Patch = [];
  const mergedLastMutationIDChanges: Record<string, number> = {};
  for (const poke of toMerge) {
    mergedPatch.push(...poke.patch);
    mergedPresence.push(...(poke.presence ?? []));
    for (const [clientID, lastMutationID] of Object.entries(
      poke.lastMutationIDChanges,
    )) {
      mergedLastMutationIDChanges[clientID] = lastMutationID;
    }
  }
  return {
    baseCookie: toMerge[0].baseCookie,
    cookie: toMerge[toMerge.length - 1].cookie,
    lastMutationIDChanges: mergedLastMutationIDChanges,
    patch: mergedPatch,
    presence: mergedPresence,
    timestamp: toMerge[0].timestamp,
  };
}
