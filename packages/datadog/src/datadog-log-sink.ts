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

// https://docs.datadoghq.com/api/latest/logs/
export const MAX_LOG_ENTRIES_PER_FLUSH = 1000;
export const FORCE_FLUSH_THRESHOLD = 250;
const MAX_ENTRY_BYTES = 5 * 1024 * 1024;

// Conservative limit that assumes all chars are encoded as 4 UTF-8 bytes.
// This makes the actual limit somewhere closer to 1.25 MB, which is still
// a reasonable amount of log data to send per request.
export const MAX_ENTRY_CHARS = MAX_ENTRY_BYTES / 4;

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
    if (level === 'error' || this._messages.length === FORCE_FLUSH_THRESHOLD) {
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

      const flushTime = Date.now();
      const stringified = [];
      let totalBytes = 0;

      for (const m of this._messages) {
        // As a small perf optimization, we directly mutate
        // the message rather than making a shallow copy.
        // The LOG_SINK_FLUSH_DELAY_ATTRIBUTE will be clobbered by
        // the next flush if this flush fails (which is the desired behavior).
        m[LOG_SINK_FLUSH_DELAY_ATTRIBUTE] = flushTime - m.date;

        let str = JSON.stringify(m);
        if (str.length > MAX_ENTRY_CHARS) {
          // A single message above the total payload limit will otherwise halt
          // log flushing progress. Drop and replace with a message indicating so.
          m.message = `[Dropped message of length ${str.length}]`;
          str = JSON.stringify(m);
        }
        // Calculate the totalBytes with the newline characters between messages.
        if (str.length + totalBytes + stringified.length > MAX_ENTRY_CHARS) {
          break;
        }
        totalBytes += str.length;
        stringified.push(str);

        if (stringified.length === MAX_LOG_ENTRIES_PER_FLUSH) {
          break;
        }
      }

      // Remove messages that have been stringified for sending.
      const flushing = this._messages.splice(0, stringified.length);

      const body = stringified.join('\n');
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
      } catch (e) {
        // Log to console so that we might catch this in `wrangler tail`.
        console.error('Log flush to datadog failed', e);
      }

      if (!ok) {
        // Put the messages back in the queue.
        this._messages.splice(0, 0, ...flushing);
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

const LOG_SINK_FLUSH_DELAY_ATTRIBUTE = 'flushDelayMs';
// This code assumes that no context keys will start with
// @DATADOG_RESERVED_ (a fairly safe assumption).
const RESERVED_KEY_PREFIX = '@DATADOG_RESERVED_';
// See https://docs.datadoghq.com/logs/log_configuration/attributes_naming_convention/#reserved-attributes
// Note 'msg' and 'date' are not documented.
// We should avoid using these as context keys.  We escape them here
// because otherwise the impact on the data dog log UI is very confusing
// (e.g. using 'msg' as a context key results, in the context value
// replacing the log message.)
const RESERVED_KEYS: ReadonlyArray<string> = [
  'host',
  'source',
  'status',
  'service',
  'trace_id',
  'message',
  'msg', // alias for message
  'date',
  // The following are attributes reserved by the DataDogLogSink
  // itself (as opposed to DataDog), to report on its own behavior.
  LOG_SINK_FLUSH_DELAY_ATTRIBUTE,
];

function makeMessage(
  message: unknown,
  context: Context | undefined,
  logLevel: LogLevel,
): Message {
  let safeContext = undefined;
  if (context !== undefined) {
    for (const reservedKey of RESERVED_KEYS) {
      if (Object.hasOwn(context, reservedKey)) {
        if (safeContext === undefined) {
          safeContext = {...context};
        }
        safeContext[RESERVED_KEY_PREFIX + reservedKey] =
          safeContext[reservedKey];
        delete safeContext[reservedKey];
      }
    }
  }
  const msg: Message = {
    ...(safeContext ?? context),
    date: Date.now(),
    message: convertErrors(flattenMessage(message)),
    status: logLevel,
  };
  if (logLevel === 'error') {
    msg.error = {origin: 'logger'};
  }
  return msg;
}
