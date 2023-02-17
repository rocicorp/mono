import type {Mutation} from '../protocol/push.js';
import type {ClientID} from './client-state.js';

export type ClientMutation = Mutation & {
  clientID: ClientID;
};
