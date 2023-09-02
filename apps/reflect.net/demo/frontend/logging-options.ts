import type {LogLevel} from '@rocicorp/logger';

const DEFAULT_LOG_LEVEL = 'info';

function getLogLevel() {
  const envLogLevel = process.env.NEXT_PUBLIC_LOG_LEVEL;
  switch (envLogLevel) {
    case 'error':
    case 'info':
    case 'debug':
      return envLogLevel;
    case undefined:
      return DEFAULT_LOG_LEVEL;
    default:
      console.log(
        'bad log level env variable value:',
        envLogLevel,
        'defaulting to:',
        DEFAULT_LOG_LEVEL,
      );
      return DEFAULT_LOG_LEVEL;
  }
}

export const loggingOptions: {logLevel: LogLevel} = {
  logLevel: getLogLevel(),
};
