import {
  DeleteFn,
  GetFn,
  ListFn,
  PatchFn,
  PostFn,
  Resource,
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
    type: string;
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
  readonly create: PostFn<CustomHostname>;
  readonly get: GetFn<CustomHostname>;
  readonly edit: PatchFn<CustomHostname>;
  readonly delete: DeleteFn<{id: string}>;

  constructor(apiToken: string, zoneID: string) {
    const resource = new Resource(
      apiToken,
      `/zones/${zoneID}/custom_hostnames`,
    );
    this.list = resource.get;
    this.create = resource.post;
    this.get = id => resource.append(id).get();
    this.edit = (id, ch) => resource.append(id).patch(ch);
    this.delete = id => resource.append(id).delete();
  }
}
