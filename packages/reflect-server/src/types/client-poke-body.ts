import type {PokeBody} from 'reflect-protocol';
import type {ClientID} from './client-state.js';

export type ClientPokeBody = {
  clientID: ClientID;
  poke: PokeBody;
};
