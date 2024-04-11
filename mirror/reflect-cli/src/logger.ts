import type {ReadonlyJSONValue} from 'shared/src/json.js';

interface Logger {
  log: typeof console.log;
  info: typeof console.info;
  debug: typeof console.debug;
  warn: typeof console.warn;
  error: typeof console.error;
  json(output: ReadonlyJSONValue): void;
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
        debug: () => {},
        warn: console.warn,
        error: console.error,
        json: (output: ReadonlyJSONValue) => {
          console.log(JSON.stringify(output, null, 2));
        },
      };
    case 'text':
      return {
        log: console.log,
        info: console.info,
        debug: console.debug,
        warn: console.warn,
        error: console.error,
        json: () => {},
      };
  }
}
