import {
  DeleteFn,
  GetFn,
  ListFn,
  SetOnlyFn,
  SetFn,
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
  readonly create: SetOnlyFn<DNSRecord>;
  readonly get: GetFn<DNSRecord>;
  readonly patch: SetFn<DNSRecord>;
  readonly update: SetFn<DNSRecord>;
  readonly delete: DeleteFn;

  constructor(apiToken: string, zoneID: string) {
    const resource = new Resource(apiToken, `/zones/${zoneID}/dns_records`);
    this.list = resource.get;
    this.create = resource.post;
    this.get = (id, q) => resource.append(id).get(q);
    this.patch = (id, val, q) => resource.append(id).patch(val, q);
    this.update = (id, val, q) => resource.append(id).put(val, q);
    this.delete = (id, q) => resource.append(id).delete(q);
  }
}
