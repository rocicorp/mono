import {SendHandle, Serializable} from 'child_process';
import EventEmitter from 'events';
import {assert} from 'shared/src/asserts.js';
import {Receiver, Sender, Worker} from './processes.js';

class FakeWorker extends EventEmitter implements Sender, Receiver {
  receiver: EventEmitter | undefined;

  send(
    message: Serializable,
    sendHandle?: SendHandle,
    callback?: (error: Error | null) => void,
  ): boolean {
    assert(this.receiver);
    this.receiver.emit('message', message, sendHandle);
    if (callback) {
      callback(null);
    }
    return true;
  }
}

/**
 * Creates a pair of {@link Worker} instances that can be used to
 * simulate communication between two processes. This is analogous
 * to a `MessageChannel` that returns two `MessagePort`s, and is useful
 * for testing inter-process protocol logic.
 */
export function fakeIPC(): [Worker, Worker] {
  const worker1 = new FakeWorker();
  const worker2 = new FakeWorker();
  worker1.receiver = worker2;
  worker2.receiver = worker1;

  return [worker1, worker2];
}
