import {default as makeOptions} from './app-module-name.js';
import {
  createReflectServer,
  defaultConsoleLogSink,
  logLevel,
  newOptionsBuilder,
} from './server-module-name.js';
const optionsBuilder = newOptionsBuilder(makeOptions)
  .add(logLevel())
  .add(defaultConsoleLogSink())
  .build();
const {worker, RoomDO, AuthDO} = createReflectServer(optionsBuilder);
export {AuthDO, RoomDO, worker as default};
