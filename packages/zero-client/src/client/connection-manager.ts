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
    };

const isSameState = (a: ConnectionState, b: ConnectionState): a is typeof b =>
  a.name === b.name;

export class ConnectionManager extends Subscribable<ConnectionState> {
  #state: ConnectionState = {
    name: ConnectionStatus.Disconnected,
  };

  get state(): ConnectionState {
    return this.#state;
  }

  setState(state: ConnectionState): boolean {
    if (isSameState(state, this.#state)) {
      return false;
    }

    this.#state = state;
    this.notify(this.#state);
    return true;
  }

  is(state: ConnectionStatus): boolean {
    const name = state;
    return this.#state.name === name;
  }
}
