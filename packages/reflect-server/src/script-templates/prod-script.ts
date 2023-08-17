import {default as makeOptions} from './app-module-name.js';
import {createReflectServer} from './server-module-name.js';
// TODO: Wrap makeOptions in logic that
// (1) Wraps app-provided LogSinks to filter internal logs.
// (2) Adds metrics and LogSinks that send full data to internal monitoring.
const {worker, RoomDO, AuthDO} = createReflectServer(makeOptions);
export {AuthDO, RoomDO, worker as default};
