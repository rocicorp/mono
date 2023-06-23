import {default as makeOptions} from '<APP>';
import {createReflectServer} from '<REFLECT_SERVER>';
const {worker, RoomDO, AuthDO} = createReflectServer(makeOptions);
export {AuthDO, RoomDO, worker as default};
