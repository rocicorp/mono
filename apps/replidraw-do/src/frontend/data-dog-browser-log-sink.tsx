import type { LogLevel, LogSink } from "@rocicorp/logger";
import { datadogLogs } from "@datadog/browser-logs";

function throwError(message: string): never {
  throw new Error(message);
}

export class DataDogBrowserLogSink implements LogSink {
  constructor() {
    datadogLogs.init({
      clientToken:
        process.env.NEXT_PUBLIC_DATADOG_CLIENT_TOKEN ??
        throwError("Missing env var NEXT_PUBLIC_DATADOG_CLIENT_TOKEN"),
      forwardErrorsToLogs: false,
      sampleRate: 100,
      silentMultipleInit: true,
    });
  }

  log(level: LogLevel, ...args: unknown[]): void {
    // The DD API says the message should be string but any json value works
    // fine and gives better logs.

    // @ts-expect-error message can be any json value.
    datadogLogs.logger.log(convertErrors(flattenMessage(args)), {}, level);
  }
}

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
