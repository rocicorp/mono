import type {Mutation} from '../protocol/push';
import type {ClientID} from './client-state';

export type ClientMutation = Mutation & {
  clientID: ClientID;
};
