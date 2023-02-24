import type {Mutation} from 'protocol/src/push.js';
import type {ClientGroupID} from './client-state.js';

export type PendingMutationMap = Map<ClientGroupID, Mutation[]>;
