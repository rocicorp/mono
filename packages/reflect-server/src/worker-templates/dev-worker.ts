import {default as makeOptions} from './app-module-name.js';
import {createReflectServer} from './server-module-name.js';
const {worker, RoomDO, AuthDO} = createReflectServer(makeOptions);
export {AuthDO, RoomDO, worker as default};
