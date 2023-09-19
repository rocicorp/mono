import {
  DeleteFn,
  GetFn,
  ListFn,
  PatchFn,
  PostFn,
  PutFn,
  Resource,
} from './resources.js';

/* eslint-disable @typescript-eslint/naming-convention */
export type DNSRecord = {
  id: string;
  zone_id: string;
  zone_name: string;
  name: string; // Record Name
  type: string; // Record Type ("A", "AAAA", "CNAME", etc.)
  content: string;
  proxied: boolean;
  ttl: number; // TTL in seconds
  tags: string[];
};
/* eslint-enable @typescript-eslint/naming-convention */

// https://developers.cloudflare.com/api/operations/dns-records-for-a-zone-list-dns-records

export class DNSRecords {
  readonly list: ListFn<DNSRecord>;
  readonly create: PostFn<DNSRecord>;
  readonly get: GetFn<DNSRecord>;
  readonly patch: PatchFn<DNSRecord>;
  readonly update: PutFn<DNSRecord>;
  readonly delete: DeleteFn<{id: string}>;

  constructor(apiToken: string, zoneID: string) {
    const resource = new Resource(apiToken, `/zones/${zoneID}/dns_records`);
    this.list = resource.get;
    this.create = resource.post;
    this.get = id => resource.append(id).get();
    this.patch = (id, ch) => resource.append(id).patch(ch);
    this.update = (id, ch) => resource.append(id).put(ch);
    this.delete = id => resource.append(id).delete();
  }
}
