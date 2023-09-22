import {DeleteFn, GetFn, ListFn, SetOnlyFn, Resource} from './resources.js';

/* eslint-disable @typescript-eslint/naming-convention */
export type DispatchNamespace = {
  namespace_id: string;
  namespace_name: string;
};
/* eslint-enable @typescript-eslint/naming-convention */

// https://developers.cloudflare.com/cloudflare-for-platforms/workers-for-platforms/get-started/dynamic-dispatch/#dispatch-namespace-api-reference

export class DispatchNamespaces {
  readonly list: ListFn<DispatchNamespace>;
  readonly create: SetOnlyFn<{name: string}, DispatchNamespace>;
  readonly get: GetFn<DispatchNamespace>;
  readonly delete: DeleteFn<null>;

  constructor(apiToken: string, accountID: string) {
    const resource = new Resource(
      apiToken,
      `/accounts/${accountID}/workers/dispatch/namespaces`,
    );
    this.list = resource.get;
    this.create = resource.post;
    this.get = (id, q) => resource.append(id).get(q);
    this.delete = (id, q) => resource.append(id).delete(q);
  }
}
