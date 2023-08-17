import {default as makeOptions} from './app-module-name.js';
import {
  createReflectServer,
  newOptionsBuilder,
  logLevel,
  defaultConsoleLogSink,
  logFilter,
} from './server-module-name.js';
const optionsBuilder = newOptionsBuilder(makeOptions)
  .add(logLevel())
  .add(defaultConsoleLogSink())
  .add(logFilter((level, ctx) => level === 'error' || ctx?.['vis'] === 'app'))
  .build();
const {worker, RoomDO, AuthDO} = createReflectServer(optionsBuilder);
export {AuthDO, RoomDO, worker as default};
