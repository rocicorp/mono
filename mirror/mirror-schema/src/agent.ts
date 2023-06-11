import * as v from 'shared/valita.js';
import * as path from './path.js';
import {firestoreDataConverter} from './converter.js';

export const agentVersionsSchema = v.object({
  minVersion: v.string(),
  deprecatedVersion: v.string(),
  currentVersion: v.string(),
});

export type AgentVersions = v.Infer<typeof agentVersionsSchema>;

export const agentVersionsDataConverter =
  firestoreDataConverter(agentVersionsSchema);

export const SUPPORTED_AGENTS_COLLECTION = 'supportedAgents';

export function agentVersionsPath(agentType: string): string {
  return path.join(SUPPORTED_AGENTS_COLLECTION, agentType);
}
