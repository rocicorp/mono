import * as v from 'shared/src/valita.js';
import {networkInterfaces} from 'os';
import {createHash} from 'crypto';

export const deviceFingerprint = computeFingerprint();

function getAllMacAddresses(): string[] {
  const allMACAddresses: string[] = [];
  const ifaces = networkInterfaces();
  for (const addresses of Object.values(ifaces)) {
    if (addresses) {
      for (const address of addresses) {
        if (
          !address.internal &&
          address.mac &&
          address.mac !== '00:00:00:00:00:00' &&
          !allMACAddresses.includes(address.mac)
        ) {
          allMACAddresses.push(address.mac);
        }
      }
    }
  }
  return allMACAddresses.sort((a, b) => a.localeCompare(b));
}

function computeFingerprint(): string {
  return createHash('md5').update(getAllMacAddresses().join(',')).digest('hex');
}

export enum UserCustomDimension {
  OsArchitecture = 'up.reflect_os_architecture',
  NodeVersion = 'up.reflect_node_version',
  ReflectCLIVersion = 'up.reflect_cli_version',
  DeviceFingerprint = 'up.reflect_device_fingerprint',
}

export const userParameterSchema = v.object({
  [UserCustomDimension.OsArchitecture]: v.string(),
  [UserCustomDimension.NodeVersion]: v.string(),
  [UserCustomDimension.ReflectCLIVersion]: v.string(),
  [UserCustomDimension.DeviceFingerprint]: v.string(),
});

export type UserParameters = v.Infer<typeof userParameterSchema>;
