import {logger} from 'firebase-functions';
import type {NamespacedScript} from 'cloudflare-api/src/scripts.js';
import {DNSRecord, DNSRecords} from 'cloudflare-api/src/dns-records.js';
import {
  CustomHostname,
  CustomHostnames,
} from 'cloudflare-api/src/custom-hostnames.js';
import type {ZoneConfig} from './config.js';
import {assert} from 'shared/src/asserts.js';
import {sleep} from 'shared/src/sleep.js';
import {Errors, FetchResultError} from 'cloudflare-api/src/fetch.js';
import type {PartialDeep} from 'type-fest';
import {HttpsError} from 'firebase-functions/v2/https';

export async function* publishCustomHostnames(
  zone: ZoneConfig,
  script: NamespacedScript,
  hostname: string,
): AsyncGenerator<string> {
  const {zoneName} = zone;
  assert(
    hostname.endsWith(`.${zoneName}`),
    `hostname must be in zone ${zoneName}`,
  );

  const records = new DNSRecords(zone);
  const currentRecords = await records.list(
    // TODO: Standardize on tags when they're enabled and all records are
    // migrated to use them. Until then, accept either tags or tags encoded
    // in comments.
    new URLSearchParams({
      tag: `script:${script.id}`,
      ['comment.contains']: `|script:${script.id}|`,
      match: 'any',
    }),
  );
  const create = new Set([hostname]);
  const discard = new Set<DNSRecord>();
  currentRecords.forEach(record => {
    if (create.has(record.name)) {
      logger.log(`Custom Hostname for ${record.name} exists`);
      create.delete(record.name);
    } else {
      discard.add(record);
    }
  });

  if (create.size + discard.size === 0) {
    return;
  }

  const hostnames = new CustomHostnames(zone);
  for (const name of create) {
    yield `Setting up hostname ${name}`;
  }
  const p: Promise<unknown>[] = [];
  discard.forEach(record =>
    p.push(deleteCustomHostname(record, records, hostnames)),
  );
  create.forEach(name =>
    p.push(createCustomHostname(name, zoneName, script, records, hostnames)),
  );
  const results = await Promise.allSettled(p);

  let error;
  results.forEach(result => {
    if (result.status === 'rejected') {
      logger.error(result.reason);
      error = result.reason;
    }
  });
  if (error) {
    throw error;
  }
}

async function createCustomHostname(
  hostname: string,
  zoneName: string,
  script: NamespacedScript,
  records: DNSRecords,
  hostnames: CustomHostnames,
): Promise<void> {
  const ch = {
    hostname,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    custom_metadata: {
      script: script.name,
      namespace: script.namespace,
    },
    ssl: {
      method: 'http',
      type: 'dv',
    },
  } as const;
  const created = await ensureCustomHostname(hostnames, ch);
  logger.log(`Created CustomHostname`, created);

  const tags = [`script:${script.id}`, `ch:${created.id}`];
  const record = await ensureDNSRecord(records, {
    name: hostname,
    type: 'CNAME',
    content: zoneName,
    proxied: true,
    // TODO: Use tags when Cloudflare enables tag support for us.
    // tags,
    // comment: 'Managed by Rocicorp (reflect.net)',
    comment: `|${tags.join('|')}|`,
  });
  logger.log(`Created DNSRecord`, record);

  let {status} = created;
  while (status !== 'active') {
    // Sending a PATCH request resets the backoff schedule to immediately revalidate.
    // https://developers.cloudflare.com/cloudflare-for-platforms/cloudflare-for-saas/security/certificate-management/issue-and-validate/validate-certificates/http/
    const state = await hostnames.edit(created.id, ch);
    if (state.status !== status) {
      status = state.status;
      logger.log(`Status of ${hostname}: ${status}`, state);
    }
    if (status !== 'active') {
      await sleep(CUSTOM_HOSTNAME_STATUS_POLL_INTERVAL);
    }
  }
}

async function ensureDNSRecord(
  records: DNSRecords,
  record: PartialDeep<DNSRecord> & {type: string; name: string},
): Promise<DNSRecord> {
  try {
    return await records.create(record);
  } catch (e) {
    FetchResultError.throwIfCodeIsNot(e, Errors.RecordWithHostAlreadyExists);
  }
  const {type, name} = record;
  const existing = await records.list(new URLSearchParams({type, name}));
  if (existing.length !== 1) {
    throw new HttpsError(
      'failed-precondition',
      `Unexpected number of dns records matching ${type} ${name}`,
      existing,
    );
  }
  const {id} = existing[0];
  logger.warn(`Updating existing DNSRecord ${id}`, existing);
  return records.update(id, record);
}

async function ensureCustomHostname(
  hostnames: CustomHostnames,
  ch: PartialDeep<CustomHostname> & {
    hostname: string;
    // eslint-disable-next-line @typescript-eslint/naming-convention
    custom_metadata: Record<string, unknown>;
  },
): Promise<CustomHostname> {
  try {
    return await hostnames.create(ch);
  } catch (e) {
    FetchResultError.throwIfCodeIsNot(e, Errors.DuplicateCustomHostnameFound);
  }
  const {hostname} = ch;
  const existing = await hostnames.list(new URLSearchParams({hostname}));
  if (existing.length !== 1) {
    throw new HttpsError(
      'failed-precondition',
      `Unexpected number of custom hostnames matching ${hostname}`,
      existing,
    );
  }
  const {id} = existing[0];
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const {custom_metadata} = ch;
  // Update the existing CustomHostname with the desired data.
  logger.warn(`Updating existing CustomHostname ${id}`, existing);
  // eslint-disable-next-line @typescript-eslint/naming-convention
  return hostnames.edit(id, {custom_metadata});
}

const CUSTOM_HOSTNAME_STATUS_POLL_INTERVAL = 1000;

function chTagFromComment(comment?: string): string | undefined {
  if (!comment) {
    return undefined;
  }
  const start = comment.indexOf('|ch:');
  if (start < 0) {
    return undefined;
  }
  const end = comment.indexOf('|', start + 1);
  if (end < 0) {
    return undefined;
  }
  const chTag = comment.substring(start + 1, end);
  logger.debug(`Found ch tag "${chTag}" in comment ${comment}`);
  return chTag;
}

async function deleteCustomHostname(
  record: DNSRecord,
  records: DNSRecords,
  hostnames: CustomHostnames,
): Promise<void> {
  logger.log(`Deleting custom hostname ${record.name}`);
  const chTag =
    (record.tags ?? []).find(tag => tag.startsWith('ch:')) ??
    chTagFromComment(record.comment);
  if (!chTag) {
    logger.warn(`No ch:<ch-id> tag for ${record.name}`, record);
  } else {
    // To avoid orphanage, wait for the CustomHostname is deleted before
    // deleting the DNSRecord.
    logger.info(`Deleting CustomHostname for ${record.name}`);
    try {
      await hostnames.delete(chTag.substring('ch:'.length));
    } catch (e) {
      // This is returned for ids that don't exist (e.g. already deleted).
      FetchResultError.throwIfCodeIsNot(e, Errors.CustomHostnameNotFound);
    }
  }

  logger.info(`Deleting DNSRecord for ${record.name}`, record);
  try {
    await records.delete(record.id);
  } catch (e) {
    FetchResultError.throwIfCodeIsNot(e, Errors.RecordDoesNotExist);
  }
}
