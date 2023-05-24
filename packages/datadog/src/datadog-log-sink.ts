import {Lock} from '@rocicorp/lock';
import type {LogLevel, LogSink, Context} from '@rocicorp/logger';

export interface DatadogLogSinkOptions {
  apiKey: string;
  source?: string | undefined;
  service?: string | undefined;
  host?: string | undefined;
  interval?: number | undefined;
}

const DD_URL = 'https://http-intake.logs.datadoghq.com/api/v2/logs';

export class DatadogLogSink implements LogSink {
  private _messages: Message[] = [];
  private readonly _apiKey: string;
  private readonly _source: string | undefined;
  private readonly _service: string | undefined;
  private readonly _host: string | undefined;
  private readonly _interval: number;
  private _timerID: ReturnType<typeof setTimeout> | 0 = 0;
  private _flushLock = new Lock();

  constructor(options: DatadogLogSinkOptions) {
    const {apiKey, source, service, host, interval = 5_000} = options;

    this._apiKey = apiKey;
    this._source = source;
    this._service = service;
    this._host = host;
    this._interval = interval;
  }

  log(level: LogLevel, context: Context | undefined, ...args: unknown[]): void {
    this._messages.push(makeMessage(args, context, level));
    if (level === 'error') {
      // Do not await. Later calls to flush will await as needed.
      void this.flush();
    } else {
      this._startTimer();
    }
  }
  private _startTimer() {
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
      const {length} = this._messages;
      if (length === 0) {
        return;
      }

      const messages = this._messages;
      this._messages = [];

      const body = messages.map(m => JSON.stringify(m)).join('\n');

      const url = new URL(DD_URL);
      url.searchParams.set('dd-api-key', this._apiKey);

      if (this._source) {
        // Both need to be set for server to treat us as the browser SDK for
        // value 'browser'.
        url.searchParams.set('ddsource', this._source);
        url.searchParams.set('dd-evp-origin', this._source);
      }

      if (this._service) {
        url.searchParams.set('service', this._service);
      }

      if (this._host) {
        url.searchParams.set('host', this._host);
      }

      let ok = false;
      try {
        const response = await fetch(url.toString(), {
          method: 'POST',
          body,
          keepalive: true,
        } as RequestInit);

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

type Message = Context & {
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
    const convertedMessage: unknown[] = [];
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

function makeMessage(
  message: unknown,
  context: Context | undefined,
  logLevel: LogLevel,
): Message {
  const msg: Message = {
    ...context,
    date: Date.now(),
    message: convertErrors(flattenMessage(message)),
    status: logLevel,
  };
  if (logLevel === 'error') {
    msg.error = {origin: 'logger'};
  }
  return msg;
}
