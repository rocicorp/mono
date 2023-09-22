import {default as makeOptions} from './app-module-name.js';
import {
  createReflectServer,
  newOptionsBuilder,
  logLevel,
  defaultConsoleLogSink,
  logFilter,
  datadogLogging,
  datadogMetrics,
} from './server-module-name.js';
const optionsBuilder = newOptionsBuilder(makeOptions)
  .add(logLevel())
  .add(defaultConsoleLogSink())
  .add(logFilter((level, ctx) => level === 'error' || ctx?.['vis'] === 'app'))
  .add(datadogLogging('app-name-team-label', 'app-script-name'))
  .add(datadogMetrics('app-name-team-label', {script: 'app-script-name'}))
  .build();
const {worker, RoomDO, AuthDO} = createReflectServer(optionsBuilder);
export {AuthDO, RoomDO, worker as default};
