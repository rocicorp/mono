import {fork, ForkOptions, SendHandle, Serializable} from 'child_process';
import EventEmitter from 'events';
import path from 'path';

/**
 * Central registry of message type names, which are used to identify
 * the payload in {@link Message} objects sent between processes. The
 * payloads themselves are implementation specific and defined in each
 * component; only the type name is reserved here to avoid collisions.
 *
 * Receiving logic can call {@link getMessage()} with the name of
 * the message of interest to filter messages to those of interest.
 */
export const MESSAGE_TYPES = {
  handoff: 'handoff',
  status: 'status',
  subscribe: 'subscribe',
  notify: 'notify',
  ready: 'ready',
} as const;

export type Message<Payload> = [keyof typeof MESSAGE_TYPES, Payload];

/**
 * Example:
 *
 * ```ts
 * type MyMessageType = [typeof MESSAGE_TYPES.handoff, { ... msg type ... }];
 *
 * worker.on('message', data => {
 *   const msg = getMessage<MyMessageType>('handoff');
 *   if (msg) {
 *     // Handle the 'handoff' message.
 *   }
 * });
 * ```
 */
export function getMessage<M extends Message<unknown>>(
  type: M[0],
  data: unknown,
): M[1] | null {
  if (Array.isArray(data) && data.length === 2 && data[0] === type) {
    return data[1] as M[1];
  }
  return null;
}

export interface Receiver {
  send<Payload>(
    message: Message<Payload>,
    sendHandle?: SendHandle,
    callback?: (error: Error | null) => void,
  ): boolean;
}

// Sub-interface of Process and ChildProcess
export interface Sender extends EventEmitter {}

export interface Worker extends Sender, Receiver {}

// Note: It is okay to cast a Processor or ChildProcess as a Worker.
// The {@link send} method simply restricts the message type for clarity.
export const parentWorker: Worker = process as Worker;

const SINGLE_PROCESS = 'SINGLE_PROCESS';

export function singleProcessMode(): boolean {
  return (process.env[SINGLE_PROCESS] ?? '0') !== '0';
}

export function childWorker(module: string, options?: ForkOptions): Worker {
  if (singleProcessMode()) {
    const [parent, child] = inProcChannel();
    void import(path.join('../../', module))
      .then(({default: runWorker}) => runWorker(parent))
      .catch(err => child.emit('error', err));
    return child;
  }
  // Note: It is okay to cast a Processor or ChildProcess as a Worker.
  // The {@link send} method simply restricts the message type for clarity.
  return fork(module, {...options, serialization: 'advanced'}) as Worker;
}

/**
 * Creates two connected `Worker` instances such that messages sent to one
 * via the {@link Worker.send()} method are received by the other's
 * `on('message', ...)` handler.
 *
 * This is analogous to the two `MessagePort`s of a `MessageChannel`, and
 * is useful for executing code written for inter-process communication
 * in a single process.
 */
export function inProcChannel(): [Worker, Worker] {
  const worker1 = new EventEmitter();
  const worker2 = new EventEmitter();

  const sendTo =
    (dest: EventEmitter) =>
    (
      message: Serializable,
      sendHandle?: SendHandle,
      callback?: (error: Error | null) => void,
    ) => {
      dest.emit('message', message, sendHandle);
      if (callback) {
        callback(null);
      }
      return true;
    };

  Object.assign(worker1, {send: sendTo(worker2)});
  Object.assign(worker2, {send: sendTo(worker1)});

  return [worker1 as Worker, worker2 as Worker];
}
