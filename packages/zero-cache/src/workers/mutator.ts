import {resolver} from '@rocicorp/resolver';
import type {Service, SingletonService} from '../services/service.ts';
import {pid} from 'node:process';
import type {Pusher} from '../services/mutagen/pusher.ts';
import {ServiceRunner} from '../services/runner.ts';
import type {LogContext} from '@rocicorp/logger';
import {WebSocketServer} from 'ws';
import {installWebSocketReceiver} from '../services/dispatcher/websocket-handoff.ts';
import type {ConnectParams} from '../services/dispatcher/connect-params.ts';
import type {WebSocket} from 'ws';
import type {Worker} from '../types/processes.ts';
import {
  Connection,
  type HandlerResult,
  type MessageHandler,
} from './connection.ts';
import type {Upstream} from '../../../zero-protocol/src/up.ts';

export class Mutator implements SingletonService {
  readonly id = `mutator-${pid}`;
  readonly #stopped;
  readonly #pushers: ServiceRunner<Pusher & Service>;
  readonly #parent: Worker;
  readonly #wss: WebSocketServer;
  readonly #connections = new Map<string, Connection>();
  readonly #lc: LogContext;

  constructor(
    lc: LogContext,
    pusherFactory: (id: string) => Pusher & Service,
    parent: Worker,
  ) {
    this.#stopped = resolver();
    this.#pushers = new ServiceRunner(lc, pusherFactory);
    this.#parent = parent;
    this.#wss = new WebSocketServer({noServer: true});
    this.#lc = lc;

    installWebSocketReceiver(this.#wss, this.#createConnection, this.#parent);
  }

  run(): Promise<void> {
    return this.#stopped.promise;
  }

  stop(): Promise<void> {
    this.#stopped.resolve();
    return this.#stopped.promise;
  }

  drain(): Promise<void> {
    this.#stopped.resolve();
    return this.#stopped.promise;
  }

  readonly #createConnection = (ws: WebSocket, params: ConnectParams) => {
    const {clientID, clientGroupID, auth} = params;
    const existing = this.#connections.get(clientID);
    if (existing) {
      existing.close(`replaced by ${params.wsID}`);
    }

    const connection = new Connection(
      this.#lc,
      params,
      ws,
      new MutatorMessageHandler(this.#pushers.getService(clientGroupID), auth),
      () => {
        if (this.#connections.get(clientID) === connection) {
          this.#connections.delete(clientID);
        }
      },
    );
    this.#connections.set(clientID, connection);
    connection.init();
  };
}

class MutatorMessageHandler implements MessageHandler {
  readonly #pusher: Pusher;
  readonly #token: string | undefined;

  constructor(pusher: Pusher, token: string | undefined) {
    this.#pusher = pusher;
    this.#token = token;
  }

  handleMessage(msg: Upstream): Promise<HandlerResult> {
    const msgType = msg[0];

    switch (msgType) {
      case 'push': {
        this.#pusher.enqueuePush(msg[1], this.#token);
        break;
      }
      default:
        throw new Error(`Unexpected message type sent to mutator: ${msgType}`);
    }

    return Promise.resolve({type: 'ok'} as const);
  }
}
