import {
  DeleteFn,
  GetFn,
  ListFn,
  SetOnlyFn,
  Resource,
  ZoneAccess,
  GetOnlyFn,
} from './resources.js';

/* eslint-disable @typescript-eslint/naming-convention */
export type CertificatePack = {
  id: string;
  type: string;
  hosts: string[];
  status: string;
  validation_method: string;
  validation_days: number;
  certificate_authority: string;
  cloudflare_branding: string;
};
/* eslint-enable @typescript-eslint/naming-convention */

export type CertificatePackQuota = {
  advanced: {
    allocated: number;
    used: number;
  };
};

// https://developers.cloudflare.com/api/operations/certificate-packs-list-certificate-packs

export class CertificatePacks {
  readonly list: ListFn<CertificatePack>;
  readonly order: SetOnlyFn<CertificatePack>;
  readonly getQuota: GetOnlyFn<CertificatePackQuota>;
  readonly get: GetFn<CertificatePack>;
  readonly delete: DeleteFn;

  constructor({apiToken, zoneID}: ZoneAccess) {
    const resource = new Resource(
      apiToken,
      `/zones/${zoneID}/ssl/certificate_packs`,
    );
    this.list = resource.get;
    this.order = resource.post;
    this.getQuota = resource.append('quota').get;
    this.get = (id, q) => resource.append(id).get(q);
    this.delete = (id, q) => resource.append(id).delete(q);
  }
}
