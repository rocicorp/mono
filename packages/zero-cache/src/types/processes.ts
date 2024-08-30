import {fork, ForkOptions, SendHandle} from 'child_process';
import {EventEmitter} from 'stream';

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

export function childWorker(module: string, options?: ForkOptions): Worker {
  // Note: It is okay to cast a Processor or ChildProcess as a Worker.
  // The {@link send} method simply restricts the message type for clarity.
  return fork(module, {...options, serialization: 'advanced'}) as Worker;
}
