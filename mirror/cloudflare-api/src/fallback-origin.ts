import {DeleteFn, GetOnlyFn, PutOnlyFn, Resource} from './resources.js';

export type FallbackOriginState = {
  origin: string;
  status: string;
  errors: string[];
};

// https://developers.cloudflare.com/api/operations/custom-hostname-fallback-origin-for-a-zone-delete-fallback-origin-for-custom-hostnames

export class FallbackOrigin {
  readonly get: GetOnlyFn<FallbackOriginState>;
  readonly update: PutOnlyFn<{origin: string}, FallbackOriginState>;
  readonly delete: DeleteFn;

  constructor(apiToken: string, zoneID: string) {
    const resource = new Resource(
      apiToken,
      `/zones/${zoneID}/custom_hostnames/fallback_origin`,
    );
    this.get = resource.get;
    this.update = resource.put;
    this.delete = id => resource.append(id).delete();
  }
}
