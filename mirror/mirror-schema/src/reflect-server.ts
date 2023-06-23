import * as v from 'shared/src/valita.js';
import {firestoreDataConverter} from './converter.js';
import * as path from './path.js';

export const moduleSchema = v.object({
  name: v.string(),
  // filename is the filename used in Google Cloud Storage. It has a GUID in it.
  filename: v.string(),
  type: v.union(v.literal('esm'), v.literal('text')),
});

export type Module = v.Infer<typeof moduleSchema>;

export const reflectServerSchema = v.object({
  main: moduleSchema,
  modules: v.array(moduleSchema),
});

export type ReflectServerModule = v.Infer<typeof reflectServerSchema>;

export const reflectServerDataConverter =
  firestoreDataConverter(reflectServerSchema);

export const REFLECT_SERVER_COLLECTION = 'servers';

export function reflectServerPath(version: string): string {
  return path.join(REFLECT_SERVER_COLLECTION, version);
}
