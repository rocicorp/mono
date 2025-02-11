import {resolver} from '@rocicorp/resolver';
import type {SingletonService} from '../services/service.ts';
import {pid} from 'node:process';

// TODO:
// - install websocket receiver
// - spin up pusher services for each unique client group that connects
export class Mutator implements SingletonService {
  readonly id = `mutator-${pid}`;
  readonly #stopped;

  constructor() {
    this.#stopped = resolver();
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
}
