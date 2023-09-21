import {DeleteFn, GetFn, ListFn, PostFn, PutFn, Resource} from './resources.js';

export type WorkerRoute = {
  id: string;
  pattern: string;
  script: string;
};

// https://developers.cloudflare.com/api/operations/worker-routes-list-routes

export class WorkerRoutes {
  readonly list: ListFn<WorkerRoute>;
  readonly create: PostFn<WorkerRoute>;
  readonly get: GetFn<WorkerRoute>;
  readonly update: PutFn<WorkerRoute>;
  readonly delete: DeleteFn;

  constructor(apiToken: string, zoneID: string) {
    const resource = new Resource(apiToken, `/zones/${zoneID}/workers/routes`);
    this.list = resource.get;
    this.create = resource.post;
    this.get = id => resource.append(id).get();
    this.update = (id, val) => resource.append(id).put(val);
    this.delete = id => resource.append(id).delete();
  }
}
