import type {ReadonlyJSONValue} from '@rocicorp/reflect';

interface Logger {
  log: typeof console.log;
  info: typeof console.info;
  warn: typeof console.warn;
  error: typeof console.error;
  json(output: ReadonlyJSONValue, level?: 'info' | 'error'): void;
}

let logger = getLoggerOfType('text');

export function getLogger() {
  return logger;
}

export function setLoggerType(type: 'json' | 'text') {
  logger = getLoggerOfType(type);
}

export function getLoggerOfType(type: 'json' | 'text'): Logger {
  switch (type) {
    case 'json':
      return {
        log: () => {},
        info: () => {},
        warn: () => {},
        error: () => {},
        json: (
          output: ReadonlyJSONValue,
          level?: 'info' | 'error' | undefined,
        ) => {
          if (level === 'info') {
            console.info(JSON.stringify(output, null, 2));
          } else if (level === 'error') {
            console.error(JSON.stringify(output, null, 2));
          }
          console.log(JSON.stringify(output, null, 2));
        },
      };
    case 'text':
      return {
        log: console.log,
        info: console.info,
        warn: console.warn,
        error: console.error,
        json: () => {},
      };
  }
}
