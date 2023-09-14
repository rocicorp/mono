const fetchPing = async (url: string): Promise<number> => {
  await fetch(url);
  const start = Date.now();
  await fetch(url);
  const end = Date.now();
  return end - start;
};

let delayWebSocketCalled = false;
export const delayWebSocket = (host: string) => {
  if (delayWebSocketCalled) {
    return;
  }
  delayWebSocketCalled = true;

  let halfTripPing = 0;
  fetchPing(`${host}/ping`)
    .then(ping => {
      halfTripPing = ping / 2;
    })
    .catch(() => {
      console.debug('Failed to fetch ping, using default value of 0ms');
    });
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const OriginalWebSocket = WebSocket;
  globalThis.WebSocket = class extends OriginalWebSocket {
    readonly #userID: string;
    readonly #sendQueue: DelayQueue<
      string | ArrayBufferLike | Blob | ArrayBufferView
    >;
    readonly #deliveryQueue: DelayQueue<MessageEvent>;

    #onMessage = (ev: MessageEvent) => {
      const latency = latencies.get(this.#userID);
      // console.log(
      //   'latency',
      //   latency,
      //   'halfTripPing',
      //   halfTripPing,
      // );
      // console.log(
      //   'onmessage',
      //   ev,
      //   'ev',
      //   latency,
      //   'latency',
      //   halfTripPing,
      //   'halfTripPing',
      // );
      this.#deliveryQueue.enqueue(ev, latency, halfTripPing);
    };

    #onMessageCallbacks: Set<(this: WebSocket, ev: MessageEvent) => unknown> =
      new Set();
    constructor(url: string | URL, protocols?: string | string[] | undefined) {
      super(url, protocols);
      const urlObj = new URL(url);
      this.#userID = urlObj.searchParams.get('clientID') ?? '';
      this.#sendQueue = new DelayQueue(data => super.send(data));
      this.#deliveryQueue = new DelayQueue(ev =>
        this.#onMessageCallbacks.forEach(cb => cb.call(this, ev)),
      );
    }

    override send(
      data: string | ArrayBufferLike | Blob | ArrayBufferView,
    ): void {
      const latency = latencies.get(this.#userID);
      this.#sendQueue.enqueue(data, latency, halfTripPing);
    }

    override addEventListener<K extends keyof WebSocketEventMap>(
      type: K,
      listener: (this: WebSocket, ev: WebSocketEventMap[K]) => unknown,
      options?: boolean | AddEventListenerOptions | undefined,
    ): void {
      if (type !== 'message') {
        super.addEventListener(type, listener, options);
        return;
      }

      const first = this.#onMessageCallbacks.size === 0;
      this.#onMessageCallbacks.add(
        listener as (this: WebSocket, ev: MessageEvent) => unknown,
      );

      if (first) {
        super.addEventListener('message', this.#onMessage);
      }
    }

    override removeEventListener<K extends keyof WebSocketEventMap>(
      type: K,
      listener: (this: WebSocket, ev: WebSocketEventMap[K]) => unknown,
      options?: boolean | EventListenerOptions | undefined,
    ): void {
      if (type !== 'message') {
        super.removeEventListener(type, listener, options);
        return;
      }
      this.#onMessageCallbacks.delete(
        listener as (this: WebSocket, ev: MessageEvent) => unknown,
      );
      if (this.#onMessageCallbacks.size === 0) {
        super.removeEventListener('message', this.#onMessage);
      }
    }
  };
};

class DelayQueue<T> {
  readonly #queue: {
    targetTime: number;
    t: T;
  }[] = [];
  #processScheduled = false;
  readonly #process: (t: T) => void;
  constructor(process: (t: T) => void) {
    this.#process = process;
  }

  enqueue(t: T, latency: number | undefined, halfTripPing: number): void {
    // console.log("enque ", t, "t", latency, "latency", halfTripPing, "halfTripPing");

    if ((latency && latency > halfTripPing) || this.#queue.length > 0) {
      const targetTime = Date.now() + Math.max(latency ?? 0 - halfTripPing, 0);
      this.#queue.push({
        targetTime,
        t,
      });
      this.#maybeScheduleProcess();
    } else {
      this.#process(t);
    }
  }

  #maybeScheduleProcess = () => {
    if (this.#processScheduled || this.#queue.length === 0) {
      return;
    }
    this.#processScheduled = true;
    setTimeout(() => {
      this.#processScheduled = false;
      const now = Date.now();
      const toProcess = [];
      for (const message of this.#queue) {
        if (message.targetTime > now) {
          break;
        }
        toProcess.push(message);
      }
      this.#queue.splice(0, toProcess.length);
      toProcess.forEach(({t}) => {
        this.#process(t);
      });
      this.#maybeScheduleProcess();
    }, this.#queue[0].targetTime - Date.now());
  };
}

const latencies: Map<string, number> = new Map();
export const setLatency = (userID: string, latency: number) => {
  console.log('setting latency for', userID, latency);
  latencies.set(userID, latency);
};
