import type {JSONType} from 'reflect-protocol';
import type {ClientGroupID} from 'replicache';
import type {ClientID} from './client-state.js';

export type PendingMutation = {
  readonly id: number;
  readonly clientID: ClientID;
  readonly clientGroupID: ClientGroupID;
  readonly pusherClientIDs: ReadonlySet<ClientID>;
  readonly name: string;
  readonly args: JSONType;
  readonly timestamp?: number | undefined;
};
