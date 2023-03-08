import type {JSONType} from 'reflect-protocol';
import type {ClientID} from './client-state.js';

export type PendingMutation = {
  id: number;
  clientID: ClientID;
  name: string;
  args: JSONType;
  timestamp?: number | undefined;
};
