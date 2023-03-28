import type {ClientGroupID} from 'replicache';
import type {ReadonlyJSONValue} from 'shared/json.js';
import type {ClientID} from './client-state.js';

export type PendingMutation = {
  readonly id: number;
  readonly clientID: ClientID;
  readonly clientGroupID: ClientGroupID;
  readonly pusherClientIDs: ReadonlySet<ClientID>;
  readonly name: string;
  readonly args: ReadonlyJSONValue;
  readonly timestamps?:
    | {
        normalizedTimestamp: number;
        originTimestamp: number;
        serverReceivedTimestamp: number;
      }
    | undefined;
};
