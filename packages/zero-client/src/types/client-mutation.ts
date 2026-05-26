import type {Mutation} from '../../../zero-protocol/src/mutation.ts';
import type {ClientID} from './client-state.ts';

export type ClientMutation = Mutation & {
  clientID: ClientID;
};
