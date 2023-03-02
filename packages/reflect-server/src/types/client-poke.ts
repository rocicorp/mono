import type {Poke, PokeBody} from 'reflect-protocol';
import type {ClientID} from './client-state.js';

export type ClientPoke = {
  clientID: ClientID;
  poke: Poke;
};

export type ClientPokeBody = {
  clientID: ClientID;
  pokeBody: PokeBody;
};
