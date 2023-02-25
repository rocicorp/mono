import type {Mutation} from 'reflect-protocol';
import type {ClientGroupID} from './client-state.js';

export type PendingMutationMap = Map<ClientGroupID, Mutation[]>;
