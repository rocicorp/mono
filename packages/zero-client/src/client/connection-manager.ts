import {Subscribable} from '../../../shared/src/subscribable.ts';
import {ConnectionStatus} from './connection-status.ts';

export type ConnectionState =
  | {
      name: ConnectionStatus.Disconnected;
    }
  | {
      name: ConnectionStatus.Connecting;
    }
  | {
      name: ConnectionStatus.Connected;
    }
  | {
      name: ConnectionStatus.Closed;
    };

export class ConnectionManager extends Subscribable<ConnectionState> {
  #state: ConnectionState = {
    name: ConnectionStatus.Disconnected,
  };

  get state(): ConnectionState {
    return this.#state;
  }

  /**
   * Updates the connection status. Returns true if the status changed.
   */
  setStatus(status: ConnectionStatus): boolean {
    // cannot transition from closed to any other status
    if (this.#state.name === ConnectionStatus.Closed) {
      return false;
    }

    if (status === this.#state.name) {
      return false;
    }

    this.#state = {name: status};
    this.notify(this.#state);
    return true;
  }

  is(status: ConnectionStatus): boolean {
    return this.#state.name === status;
  }
}
