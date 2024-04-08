import {Version} from '../../types.js';
import {Request} from '../message.js';

export interface Operator {
  /**
   * Notify along the graph that the transaction
   * has been committed
   */
  commit(version: Version): void;
  messageUpstream(message: Request): void;
  destroy(): void;
}

export class NoOp implements Operator {
  constructor() {}
  commit(_v: Version): void {}
  messageUpstream(): void {}
  destroy(): void {}
}
