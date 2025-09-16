/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
