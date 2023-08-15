import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import {timestampSchema} from './timestamp.js';
import * as path from './path.js';
import {appPath} from './app.js';

export const lockSchema = v.object({
  holder: v.string(),
  expiration: timestampSchema,
});

export type LockDoc = v.Infer<typeof lockSchema>;

export const lockDataConverter = firestoreDataConverter(lockSchema);

export function deploymentLockPath(appID: string): string {
  return path.append(appPath(appID), 'locks', 'deployment');
}
