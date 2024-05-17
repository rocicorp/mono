import type {Mutation} from 'zero-protocol';
import type {ClientID} from './client-state.js';

export type ClientMutation = Mutation & {
  clientID: ClientID;
};
