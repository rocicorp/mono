import type {LogContext} from '@rocicorp/logger';
import {nanoid} from 'nanoid';
import * as v from '../../../../shared/src/valita.js';

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
      return taskID;
    } catch (e) {
      lc.warn?.('unable to determine task ID. falling back to random ID', e);
    }
  }
  return nanoid();
}
