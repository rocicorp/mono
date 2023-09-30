import * as v from 'shared/src/valita.js';
import {networkInterfaces, arch} from 'os';
import {createHash} from 'crypto';

export const deviceFingerprint = computeFingerprint();

function computeFingerprint(): string {
  return createHash('md5')
    .update(JSON.stringify(networkInterfaces()))
    .digest('hex');
}

export const primativeTypeSchema = v.union(v.string(), v.number(), v.boolean());

export type PrimitiveTypes = v.Infer<typeof primativeTypeSchema>;

export enum UserCustomDimension {
  OsArchitecture = 'up.reflect_os_architecture',
  NodeVersion = 'up.reflect_node_version',
  ReflectCLIVersion = 'up.reflect_cli_version',
  DeviceFingerprint = 'up.reflect_device_fingerprint',
}

export const userParamaeterSchema = v.object({
  [UserCustomDimension.OsArchitecture]: primativeTypeSchema,
  [UserCustomDimension.NodeVersion]: primativeTypeSchema,
  [UserCustomDimension.ReflectCLIVersion]: primativeTypeSchema,
  [UserCustomDimension.DeviceFingerprint]: primativeTypeSchema,
});

export type UserParameters = v.Infer<typeof userParamaeterSchema>;

export function getUserParameters(version: string): UserParameters {
  return {
    [UserCustomDimension.OsArchitecture]: arch(),
    [UserCustomDimension.NodeVersion]: process.version,
    [UserCustomDimension.ReflectCLIVersion]: version,
    [UserCustomDimension.DeviceFingerprint]: deviceFingerprint,
  };
}
