import type {BaseRoomDO} from './room-do.js';
import {
  RociRequest,
  RociRouter,
  requireAuthAPIKeyMatches,
} from './middleware.js';
import type {MutatorDefs} from 'replicache';

type Route = {
  path: string;
  add: (
    router: RociRouter,
    roomDO: BaseRoomDO<MutatorDefs>,
    authApiKey: string | undefined,
  ) => void;
};
const routes: Route[] = [];

// Note: paths may have router-style path parameters, e.g. /foo/:bar.
export function paths() {
  return routes.map(route => route.path);
}

// Called by the roomDO to set up its routes.
export function addRoutes(
  router: RociRouter,
  roomDO: BaseRoomDO<MutatorDefs>,
  authApiKey: string | undefined,
) {
  routes.forEach(route => route.add(router, roomDO, authApiKey));
}

// Currently this deletePath is the same in the authDO and roomDO.
export const deletePath = '/api/room/v0/room/:roomID/delete';
routes.push({
  path: deletePath,
  add: (
    router: RociRouter,
    roomDO: BaseRoomDO<MutatorDefs>,
    authApiKey: string | undefined,
  ) => {
    router.post(
      deletePath,
      requireAuthAPIKeyMatches(authApiKey),
      (_: RociRequest) =>
        // TODO: should plumb a LogContext in here.
        roomDO.deleteAllData(),
    );
  },
});
