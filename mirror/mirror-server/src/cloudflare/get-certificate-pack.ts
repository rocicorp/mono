import {cfFetch} from './cf-fetch.js';
import type {ZoneConfig} from './config.js';

// Assumed from https://developers.cloudflare.com/api/operations/custom-hostname-for-a-zone-list-custom-hostnames
export type CertificateStatus =
  | 'initializing'
  | 'pending_validation'
  | 'deleted'
  | 'pending_issuance'
  | 'pending_deployment'
  | 'pending_deletion'
  | 'pending_expiration'
  | 'expired'
  | 'active'
  | 'initializing_timed_out'
  | 'validation_timed_out'
  | 'issuance_timed_out'
  | 'deployment_timed_out'
  | 'deletion_timed_out'
  | 'pending_cleanup'
  | 'staging_deployment'
  | 'staging_active'
  | 'deactivating'
  | 'inactive'
  | 'backup_issued'
  | 'holding_deployment';

// API call is at https://developers.cloudflare.com/api/operations/certificate-packs-get-certificate-pack
// but the return type is not at all documented.
export type CertificatePack = {
  id: string;
  type: string;
  hosts: string[];
  status: CertificateStatus;

  // These fields show up but we currently are not interested in.
  // primary_certificate: '0';
  // certificates: [];
  // created_on: '2023-09-07T23:03:26.996316Z';
  // validity_days: 90;
  // validation_method: 'txt';
  // validation_records: [[Object], [Object]];
  // certificate_authority: 'lets_encrypt';
};

export function getCertificatePack(
  {apiToken, zoneID}: ZoneConfig,
  certID: string,
): Promise<CertificatePack> {
  return cfFetch<CertificatePack>(
    apiToken,
    `/zones/${zoneID}/ssl/certificate_packs/${certID}`,
    {
      headers: {
        'Content-Type': 'application/json',
      },
    },
  );
}
