import type {Mutation} from 'zero-protocol/src/mod.js';
import type {ClientID} from './client-state.js';

export type ClientMutation = Mutation & {
  clientID: ClientID;
};
