import type {PokeBody} from '../protocol/poke';

export function mergePokeBodies(pokes: PokeBody[]): PokeBody {
  if (pokes.length === 0) {
    throw new Error('Cannot merge empty array of pokes');
  }

  const patch = pokes.flatMap((poke) => poke.patch);
  const first = pokes[0];
  const last = pokes[pokes.length - 1];

  return {
    baseCookie: first.baseCookie,
    cookie: last.cookie,
    lastMutationID: last.lastMutationID,
    patch,
    epoch: first.epoch,
    frame: first.frame,
  };
}
