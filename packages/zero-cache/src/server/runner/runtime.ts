/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import type {LogContext} from '@rocicorp/logger';
import {nanoid} from 'nanoid';
import * as v from '../../../../shared/src/valita.ts';

// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-metadata-endpoint-v4-response.html
const containerMetadataSchema = v.object({['TaskARN']: v.string()});

export async function getTaskID(lc: LogContext) {
  const containerURI = process.env['ECS_CONTAINER_METADATA_URI_V4'];
  if (containerURI) {
    try {
      const resp = await fetch(`${containerURI}`);
      const metadata = await resp.json();
      // Logged purely for debugging.
      lc.info?.(`Container metadata`, {metadata});
    } catch (e) {
      lc.warn?.('unable to lookup container metadata', e);
    }

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
