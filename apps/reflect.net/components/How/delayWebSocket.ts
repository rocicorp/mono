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
    private readonly _userID: string;
    private readonly _sendQueue: DelayQueue<
      string | ArrayBufferLike | Blob | ArrayBufferView
    >;
    private readonly _deliveryQueue: DelayQueue<MessageEvent>;

    private _onMessage = (ev: MessageEvent) => {
      const latency = latencies.get(this._userID);
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
      this._deliveryQueue.enqueue(ev, latency, halfTripPing);
    };

    private _onMessageCallbacks: Set<
      (this: WebSocket, ev: MessageEvent) => any
    > = new Set();
    constructor(url: string | URL, protocols?: string | string[] | undefined) {
      super(url, protocols);
      const urlObj = new URL(url);
      this._userID = urlObj.searchParams.get('clientID') ?? '';
      this._sendQueue = new DelayQueue(data => super.send(data));
      this._deliveryQueue = new DelayQueue(ev =>
        this._onMessageCallbacks.forEach(cb => cb.call(this, ev)),
      );
    }

    override send(
      data: string | ArrayBufferLike | Blob | ArrayBufferView,
    ): void {
      const latency = latencies.get(this._userID);
      this._sendQueue.enqueue(data, latency, halfTripPing);
    }

    override addEventListener<K extends keyof WebSocketEventMap>(
      type: K,
      listener: (this: WebSocket, ev: WebSocketEventMap[K]) => any,
      options?: boolean | AddEventListenerOptions | undefined,
    ): void {
      if (type !== 'message') {
        super.addEventListener(type, listener, options);
        return;
      }

      const first = this._onMessageCallbacks.size === 0;
      this._onMessageCallbacks.add(
        listener as (this: WebSocket, ev: MessageEvent) => any,
      );

      if (first) {
        super.addEventListener('message', this._onMessage);
      }
    }

    override removeEventListener<K extends keyof WebSocketEventMap>(
      type: K,
      listener: (this: WebSocket, ev: WebSocketEventMap[K]) => any,
      options?: boolean | EventListenerOptions | undefined,
    ): void {
      if (type !== 'message') {
        super.removeEventListener(type, listener, options);
        return;
      }
      this._onMessageCallbacks.delete(
        listener as (this: WebSocket, ev: MessageEvent) => any,
      );
      if (this._onMessageCallbacks.size === 0) {
        super.removeEventListener('message', this._onMessage);
      }
    }
  };
};

class DelayQueue<T> {
  private readonly _queue: {
    targetTime: number;
    t: T;
  }[] = [];
  private _processScheduled = false;
  private readonly _process: (t: T) => void;
  constructor(process: (t: T) => void) {
    this._process = process;
  }

  enqueue(t: T, latency: number | undefined, halfTripPing: number): void {
    // console.log("enque ", t, "t", latency, "latency", halfTripPing, "halfTripPing");

    if ((latency && latency > halfTripPing) || this._queue.length > 0) {
      const targetTime = Date.now() + Math.max(latency ?? 0 - halfTripPing, 0);
      this._queue.push({
        targetTime,
        t,
      });
      this._maybeScheduleProcess();
    } else {
      this._process(t);
    }
  }

  private _maybeScheduleProcess = () => {
    if (this._processScheduled || this._queue.length === 0) {
      return;
    }
    this._processScheduled = true;
    setTimeout(() => {
      this._processScheduled = false;
      const now = Date.now();
      const toProcess = [];
      for (const message of this._queue) {
        if (message.targetTime > now) {
          break;
        }
        toProcess.push(message);
      }
      this._queue.splice(0, toProcess.length);
      toProcess.forEach(({t}) => {
        this._process(t);
      });
      this._maybeScheduleProcess();
    }, this._queue[0].targetTime - Date.now());
  };
}

const latencies: Map<string, number> = new Map();
export const setLatency = (userID: string, latency: number) => {
  console.log('setting latency for', userID, latency);
  latencies.set(userID, latency);
};
