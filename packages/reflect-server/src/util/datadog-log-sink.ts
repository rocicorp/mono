import {Lock} from '@rocicorp/lock';
import type {LogLevel, LogSink} from '@rocicorp/logger';

export interface DatadogLoggerOptions {
  apiKey: string;
  service?: string;
  host?: string;
  interval?: number;
  signal?: AbortSignal;
}

const DD_URL = 'https://http-intake.logs.datadoghq.com/api/v2/logs';

export class DatadogLogSink implements LogSink {
  private _messages: Message[] = [];
  private readonly _apiKey: string;
  private readonly _service: string | undefined;
  private readonly _host: string | undefined;
  private readonly _interval: number;
  private _timerID: ReturnType<typeof setTimeout> | 0 = 0;
  private _flushLock = new Lock();
  private readonly _signal: AbortSignal | null = null;

  constructor(options: DatadogLoggerOptions) {
    const {apiKey, service, host, interval = 10_000, signal = null} = options;

    this._apiKey = apiKey;
    this._service = service;
    this._host = host;
    this._interval = interval;
    this._signal = signal;

    if (signal) {
      // CF types declarations are not correct.
      (signal as unknown as EventTarget).addEventListener('abort', () => {
        if (this._timerID) {
          clearTimeout(this._timerID);
        }
      });
    }
  }

  log(level: LogLevel, ...args: unknown[]): void {
    if (this._signal?.aborted) {
      return;
    }

    this._messages.push(makeMessage(args, level));
    if (level === 'error') {
      // Do not await. Later calls to flush will await as needed.
      void this.flush();
    } else {
      this._startTimer();
    }
  }
  private _startTimer() {
    if (this._signal?.aborted) {
      return;
    }

    if (this._timerID) {
      return;
    }

    this._timerID = setTimeout(() => {
      this._timerID = 0;

      void this.flush();
    }, this._interval);
  }

  flush(): Promise<void> {
    return this._flushLock.withLock(async () => {
      if (this._signal?.aborted) {
        return;
      }

      const {length} = this._messages;
      if (length === 0) {
        return;
      }

      const messages = this._messages;
      this._messages = [];

      const body = messages.map(m => JSON.stringify(m)).join('\n');

      const url = new URL(DD_URL);
      url.searchParams.set('ddsource', 'worker');
      this._service && url.searchParams.set('service', this._service);
      this._host && url.searchParams.set('host', this._host);

      let ok = false;
      try {
        const response = await fetch(url.toString(), {
          method: 'POST',
          headers: {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            'DD-API-KEY': this._apiKey,
          },
          body,
          signal: this._signal,
        });

        ok = response.ok;
      } catch {
        // ok stays false
      }

      if (!ok) {
        // Put the messages back in the queue.
        this._messages.splice(0, 0, ...messages);
      }

      // If any messages left at this point schedule another flush.
      if (this._messages.length) {
        this._startTimer();
      }
    });
  }
}

type Message = {
  status: LogLevel;
  date: number;
  message: unknown;
  error?: {origin: 'logger'};
};

function flattenMessage(message: unknown): unknown {
  if (Array.isArray(message) && message.length === 1) {
    return flattenMessage(message[0]);
  }
  return message;
}

function convertError(e: Error): {
  name: string;
  message: string;
  stack: string | undefined;
} {
  return {
    name: e.name,
    message: e.message,
    stack: e.stack,
  };
}

function convertErrors(message: unknown): unknown {
  if (message instanceof Error) {
    return convertError(message);
  }
  if (message instanceof Array) {
    const convertedMessage = [];
    for (const item of message) {
      if (item instanceof Error) {
        convertedMessage.push(convertError(item));
      } else {
        convertedMessage.push(item);
      }
    }
    return convertedMessage;
  }
  return message;
}

function makeMessage(message: unknown, logLevel: LogLevel): Message {
  const msg: Message = {
    date: Date.now(),
    message: convertErrors(flattenMessage(message)),
    status: logLevel,
  };
  if (logLevel === 'error') {
    msg.error = {origin: 'logger'};
  }
  return msg;
}
