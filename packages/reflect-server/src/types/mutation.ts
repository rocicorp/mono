import type {Mutation} from '../protocol/push.js';
import type {ClientGroupID} from './client-state.js';

export type PendingMutationMap = Map<ClientGroupID, Mutation[]>;
