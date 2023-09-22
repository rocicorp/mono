import {
  DeleteFn,
  GetFn,
  ListFn,
  SetOnlyFn,
  SetFn,
  Resource,
} from './resources.js';

export type WorkerRoute = {
  id: string;
  pattern: string;
  script: string;
};

// https://developers.cloudflare.com/api/operations/worker-routes-list-routes

export class WorkerRoutes {
  readonly list: ListFn<WorkerRoute>;
  readonly create: SetOnlyFn<WorkerRoute>;
  readonly get: GetFn<WorkerRoute>;
  readonly update: SetFn<WorkerRoute>;
  readonly delete: DeleteFn;

  constructor(apiToken: string, zoneID: string) {
    const resource = new Resource(apiToken, `/zones/${zoneID}/workers/routes`);
    this.list = resource.get;
    this.create = resource.post;
    this.get = (id, q) => resource.append(id).get(q);
    this.update = (id, val, q) => resource.append(id).put(val, q);
    this.delete = (id, q) => resource.append(id).delete(q);
  }
}
