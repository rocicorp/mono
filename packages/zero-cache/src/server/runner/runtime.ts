import type {LogContext} from '@rocicorp/logger';
import {nanoid} from 'nanoid';
import {networkInterfaces} from 'node:os';
import * as v from '../../../../shared/src/valita.ts';

// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v4-response.html
const containerMetadataSchema = v.object({['TaskARN']: v.string()});

export async function getTaskID(lc: LogContext) {
  const containerURI = process.env['ECS_CONTAINER_METADATA_URI_V4'];
  if (containerURI) {
    try {
      const resp = await fetch(`${containerURI}/task`);
      const metadata = v.parse(
        await resp.json(),
        containerMetadataSchema,
        'passthrough',
      );
      lc.info?.(`Task metadata`, {metadata});
      const {TaskARN: taskID} = metadata;
      // Task ARN's are long, e.g.
      // "arn:aws:ecs:us-east-1:712907626835:task/zbugs-prod-Cluster-vvNFcPUVpGHr/0042ea25bf534dc19975e26f61441737"
      // We only care about the unique ID, i.e. the last path component.
      const lastSlash = taskID.lastIndexOf('/');
      return taskID.substring(lastSlash + 1); // works for lastSlash === -1 too
    } catch (e) {
      lc.warn?.('unable to determine task ID. falling back to random ID', e);
    }
  }
  return nanoid();
}

export async function getHostIp(lc: LogContext) {
  const interfaces = Object.values(networkInterfaces())
    .flat()
    .filter(val => val !== undefined)
    .filter(val => !val.internal);
  interfaces.sort((a, b) => {
    if (a.family !== b.family) {
      return a.family === 'IPv4' ? -1 : 1;
    }
    // arbitrary
    return a.address.localeCompare(b.address);
  });

  lc.info?.(`Network interfaces`, {interfaces});

  const containerURI = process.env['ECS_CONTAINER_METADATA_URI_V4'];
  if (containerURI) {
    try {
      const resp = await fetch(`${containerURI}`);
      const metadata = await resp.json();
      lc.info?.(`Container metadata`, {metadata});
    } catch (e) {
      lc.warn?.('unable to determine host IP', e);
    }
  }
}
