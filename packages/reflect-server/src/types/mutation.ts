import type {Mutation} from 'protocol';
import type {ClientGroupID} from './client-state.js';

export type PendingMutationMap = Map<ClientGroupID, Mutation[]>;
