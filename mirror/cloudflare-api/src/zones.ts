import {GetFn, ListFn, Resource} from './resources.js';

export type Zone = {
  id: string;
  name: string;
  permissions: string[];
};

// https://developers.cloudflare.com/api/operations/zones-get

export class Zones {
  readonly list: ListFn<Zone>;
  readonly get: GetFn<Zone>;

  constructor(apiToken: string) {
    const resource = new Resource(apiToken, `/zones`);
    this.list = resource.get;
    this.get = (id, q) => resource.append(id).get(q);
  }
}
