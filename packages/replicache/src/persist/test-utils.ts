import type {ClientGroupID} from '../sync/ids.ts';
import type {ClientGroup, ClientGroupMap} from './client-groups.ts';

export type PartialClientGroup = Partial<ClientGroup> &
  Pick<ClientGroup, 'headHash'>;

export function makeClientGroup(
  partialClientGroup: PartialClientGroup,
): ClientGroup {
  return {
    mutatorNames: [],
    indexes: {},
    mutationIDs: {},
    lastServerAckdMutationIDs: {},
    disabled: false,
    ...partialClientGroup,
  };
}

export function makeClientGroupMap(
  partialClientGroups: Record<ClientGroupID, PartialClientGroup>,
): ClientGroupMap {
  const clientGroupMap = new Map();
  for (const [clientGroupID, partialClientGroup] of Object.entries(
    partialClientGroups,
  )) {
    clientGroupMap.set(clientGroupID, makeClientGroup(partialClientGroup));
  }
  return clientGroupMap;
}
