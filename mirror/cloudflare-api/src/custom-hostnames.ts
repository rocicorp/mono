import {
  DeleteFn,
  GetFn,
  ListFn,
  SetOnlyFn,
  Resource,
  SetFn,
} from './resources.js';

/* eslint-disable @typescript-eslint/naming-convention */
export type CustomHostname = {
  id: string;
  hostname: string;
  status: string;
  custom_metadata: Record<string, unknown>;
  ssl: {
    bundle_method: 'ubiquitous' | 'optimal' | 'force';
    certificate_authority: 'digicert' | 'google' | 'lets_encrypt';
    method: 'http' | 'txt' | 'email';
    status: string;
    type: 'dv';
    settings: {
      min_tls_version: '1.0' | '1.1' | '1.2' | '1.3';
      http2: 'on' | 'off';
      wildcard: boolean;
    };
  };
};
/* eslint-enable @typescript-eslint/naming-convention */

// https://developers.cloudflare.com/api/operations/custom-hostname-for-a-zone-list-custom-hostnames

export class CustomHostnames {
  readonly list: ListFn<CustomHostname>;
  readonly create: SetOnlyFn<CustomHostname>;
  readonly get: GetFn<CustomHostname>;
  readonly edit: SetFn<CustomHostname>;
  readonly delete: DeleteFn;

  constructor(apiToken: string, zoneID: string) {
    const resource = new Resource(
      apiToken,
      `/zones/${zoneID}/custom_hostnames`,
    );
    this.list = resource.get;
    this.create = resource.post;
    this.get = (id, q) => resource.append(id).get(q);
    this.edit = (id, val, q) => resource.append(id).patch(val, q);
    this.delete = (id, q) => resource.append(id).delete(q);
  }
}
