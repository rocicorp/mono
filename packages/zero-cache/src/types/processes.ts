import {
  type ChildProcess,
  fork,
  type SendHandle,
  type Serializable,
} from 'node:child_process';
import EventEmitter from 'node:events';
import {platform} from 'node:os';
import {pid} from 'node:process';

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

function getMessage<M extends Message<unknown>>(
  type: M[0],
  data: unknown,
): M[1] | null {
  if (Array.isArray(data) && data.length === 2 && data[0] === type) {
    return data[1] as M[1];
  }
  return null;
}

function onMessageType<M extends Message<unknown>>(
  e: EventEmitter,
  type: M[0],
  handler: (msg: M[1], sendHandle?: SendHandle) => void,
) {
  return e.on('message', (data, sendHandle) => {
    const msg = getMessage(type, data);
    if (msg) {
      handler(msg, sendHandle);
    }
  });
}

function onceMessageType<M extends Message<unknown>>(
  e: EventEmitter,
  type: M[0],
  handler: (msg: M[1], sendHandle?: SendHandle) => void,
) {
  const listener = (data: unknown, sendHandle: SendHandle) => {
    const msg = getMessage(type, data);
    if (msg) {
      e.off('message', listener);
      handler(msg, sendHandle);
    }
  };
  return e.on('message', listener);
}

export interface Sender {
  send<M extends Message<unknown>>(
    message: M,
    sendHandle?: SendHandle,
    callback?: (error: Error | null) => void,
  ): boolean;

  kill(signal?: NodeJS.Signals): void;
}

export interface Subprocess extends Sender, EventEmitter {
  pid?: number | undefined;
}

export interface Receiver extends EventEmitter {
  /**
   * The receiving side of {@link Sender.send()} that is a wrapper around
   * {@link on}('message', ...) that invokes the `handler` for messages of
   * the specified `type`.
   */
  onMessageType<M extends Message<unknown>>(
    type: M[0],
    handler: (msg: M[1], sendHandle?: SendHandle) => void,
  ): this;

  /**
   * The receiving side of {@link Sender.send()} that behaves like
   * {@link once}('message', ...) that invokes the `handler` for the next
   * message of the specified `type` and then unsubscribes.
   */
  onceMessageType<M extends Message<unknown>>(
    type: M[0],
    handler: (msg: M[1], sendHandle?: SendHandle) => void,
  ): this;
}

export interface Worker extends Subprocess, Receiver {}

/**
 * Adds the {@link Receiver.onMessageType()} and {@link Receiver.onceMessageType()}
 * methods to convert the given `EventEmitter` to a `Receiver`.
 */
function wrap<P extends EventEmitter>(proc: P): P & Receiver {
  return new Proxy(proc, {
    get(target: P, prop: string | symbol, receiver: unknown) {
      switch (prop) {
        case 'onMessageType':
          return (
            type: keyof typeof MESSAGE_TYPES,
            handler: (msg: unknown, sendHandle?: SendHandle) => void,
          ) => {
            onMessageType(target, type, handler);
            return receiver; // this
          };
        case 'onceMessageType':
          return (
            type: keyof typeof MESSAGE_TYPES,
            handler: (msg: unknown, sendHandle?: SendHandle) => void,
          ) => {
            onceMessageType(target, type, handler);
            return receiver; // this
          };
      }
      return Reflect.get(target, prop, receiver);
    },
  }) as P & Receiver;
}

type Proc = Pick<ChildProcess, 'send' | 'kill' | 'pid'> & EventEmitter;

/**
 * The parentWorker for forked processes, or `null` if the process was not forked.
 * (Analogous to the `parentPort: MessagePort | null` of the `"workers"` library).
 */
export const parentWorker: Worker | null = process.send
  ? wrap(process as Proc)
  : null;

const SINGLE_PROCESS = 'SINGLE_PROCESS';
let singleProcessOverride = false;
export function singleProcessMode(): boolean {
  return singleProcessOverride || (process.env[SINGLE_PROCESS] ?? '0') !== '0';
}

export function setSingleProcessMode(enabled: boolean = true): void {
  singleProcessOverride = enabled;
}

/**
 *
 * @param modulePath Path to the module file, relative to zero-cache/src/, or an absolute file:// URL
 */
export function childWorker(
  moduleUrl: URL,
  env?: NodeJS.ProcessEnv,
  ...args: string[]
): Worker {
  args.push(...process.argv.slice(2));

  if (singleProcessMode()) {
    const [parent, child] = inProcChannel();
    import(moduleUrl.href)
      .then(async ({default: runWorker}) => {
        try {
          await runWorker(parent, env ?? process.env, ...args);
          child.emit('close', 0);
          return;
        } catch (err) {
          child.emit('error', err);
          child.emit('close', -1);
        }
      })
      .catch(err => child.emit('error', err));
    return child;
  }
  const child = fork(moduleUrl, args, {
    // For production / non-windows, set `detached` to `true` so that SIGINT is
    // not automatically propagated and graceful shutdown happens as intended.
    // For Win32, detached: true causes all subprocesses to open in separate
    // terminals and breaks inter-process kill signals, so set it to false.
    detached: platform() !== 'win32',
    serialization: 'advanced', // use structured clone for IPC
    env,
  });
  return wrap(child);
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

  const kill =
    (dest: EventEmitter) =>
    (signal: NodeJS.Signals = 'SIGTERM') =>
      dest.emit(signal, signal);

  return [
    wrap(
      Object.assign(worker1, {send: sendTo(worker2), kill: kill(worker2), pid}),
    ),
    wrap(
      Object.assign(worker2, {send: sendTo(worker1), kill: kill(worker1), pid}),
    ),
  ];
}
