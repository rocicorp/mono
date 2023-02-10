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
    datadogLogs.logger.log(args.join(", "), {}, level);
  }
}
