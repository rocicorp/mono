/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import type {LogContext} from '@rocicorp/logger';
import {isIPv6, isPrivate, isReserved} from 'is-in-subnet';
import {networkInterfaces, type NetworkInterfaceInfo} from 'os';

export function getHostIp(lc: LogContext, preferredPrefixes: string[]) {
  const interfaces = networkInterfaces();
  const preferred = getPreferredIp(interfaces, preferredPrefixes);
  lc.info?.(`network interfaces`, {preferred, interfaces});
  return preferred;
}

export function getPreferredIp(
  interfaces: NodeJS.Dict<NetworkInterfaceInfo[]>,
  preferredPrefixes: string[],
) {
  const rank = ({name}: {name: string}) => {
    for (let i = 0; i < preferredPrefixes.length; i++) {
      if (name.startsWith(preferredPrefixes[i])) {
        return i;
      }
    }
    return Number.MAX_SAFE_INTEGER;
  };

  const sorted = Object.entries(interfaces)
    .map(([name, infos]) => (infos ?? []).map(info => ({...info, name})))
    .flat()
    .sort((a, b) => {
      const ap =
        (isIPv6(a.address) && isPrivate(a.address)) || isReserved(a.address);
      const bp =
        (isIPv6(b.address) && isPrivate(b.address)) || isReserved(b.address);
      if (ap !== bp) {
        // Avoid link-local, site-local, or otherwise private addresses
        return ap ? 1 : -1;
      }
      if (a.internal !== b.internal) {
        // Prefer non-internal addresses.
        return a.internal ? 1 : -1;
      }
      if (a.family !== b.family) {
        // Prefer IPv4.
        return a.family === 'IPv4' ? -1 : 1;
      }
      const rankA = rank(a);
      const rankB = rank(b);
      if (rankA !== rankB) {
        return rankA - rankB;
      }
      // arbitrary
      return a.address.localeCompare(b.address);
    });

  // Enclose IPv6 addresses in square brackets for use in a URL.
  const preferred =
    sorted[0].family === 'IPv4' ? sorted[0].address : `[${sorted[0].address}]`;
  return preferred;
}
