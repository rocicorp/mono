import {GetFn, ListFn, Resource} from './resources.js';

export type Account = {
  id: string;
  name: string;
};

// https://developers.cloudflare.com/api/operations/accounts-list-accounts

export class Accounts {
  readonly list: ListFn<Account>;
  readonly get: GetFn<Account>;

  constructor(apiToken: string) {
    const resource = new Resource(apiToken, `/accounts`);
    this.list = resource.get;
    this.get = (id, q) => resource.append(id).get(q);
  }
}
