import type {Mutation} from 'src/protocol/push.js';
import type {ClientGroupID} from './client-state.js';

export type PendingMutationMap = Map<ClientGroupID, Mutation[]>;
