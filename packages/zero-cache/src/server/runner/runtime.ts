import type {LogContext} from '@rocicorp/logger';
import {nanoid} from 'nanoid';
import * as v from '../../../../shared/src/valita.ts';

// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v4-response.html
const containerMetadataSchema = v.object({['TaskARN']: v.string()});

export async function getTaskID(lc: LogContext) {
  const containerURI = process.env['ECS_CONTAINER_METADATA_URI_V4'];
  if (containerURI) {
    try {
      const resp = await fetch(`${containerURI}/task`);
      const {TaskARN: taskID} = v.parse(
        await resp.json(),
        containerMetadataSchema,
        'passthrough',
      );
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
