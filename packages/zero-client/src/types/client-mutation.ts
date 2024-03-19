import type {Mutation} from 'reflect-protocol';
import type {ClientID} from './client-state.js';

export type ClientMutation = Mutation & {
  clientID: ClientID;
};
