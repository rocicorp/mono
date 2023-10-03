import * as v from 'shared/src/valita.js';
import {networkInterfaces} from 'os';
import {createHash} from 'crypto';

export const deviceFingerprint = computeFingerprint();

function computeFingerprint(): string {
  return createHash('md5')
    .update(
      JSON.stringify(
        networkInterfaces(),
        Object.keys(networkInterfaces()).sort(),
      ),
    )
    .digest('hex');
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
