import type {Mutation} from '../../../zero-protocol/src/push.ts';
import type {ClientID} from './client-state.ts';

export type ClientMutation = Mutation & {
  clientID: ClientID;
};
