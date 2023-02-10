import type {PokeBody} from '../protocol/poke.js';
import type {ClientID} from './client-state.js';

export type ClientPokeBody = {
  clientID: ClientID;
  poke: PokeBody;
};
