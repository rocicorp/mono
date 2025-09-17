/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members, @typescript-eslint/prefer-promise-reject-errors */
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
