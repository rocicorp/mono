import type {Poke} from '../protocol/poke.js';
import type {ClientID} from './client-state.js';

export type ClientPoke = {
  clientID: ClientID;
  poke: Poke;
};
