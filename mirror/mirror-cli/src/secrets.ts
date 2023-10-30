import {SecretManagerServiceClient} from '@google-cloud/secret-manager';
import {GrpcStatus} from 'firebase-admin/firestore';

const secrets = new SecretManagerServiceClient();

export async function getSecret(
  stack: string,
  id: string,
): Promise<{payload: string; version: string}> {
  const resource = `projects/reflect-mirror-${stack}/secrets/${id}/versions/latest`;
  const [{name: path, payload}] = await secrets.accessSecretVersion({
    name: resource,
  });
  if (!path || !payload || !payload.data) {
    throw new Error(`No data for ${id} secret`);
  }
  const parts = path.split('/');
  const {data} = payload;
  return {
    version: parts[parts.length - 1],
    payload: typeof data === 'string' ? data : new TextDecoder().decode(data),
  };
}

export async function storeSecret(
  stack: string,
  secretId: string,
  val: string,
  addNextVersion = false,
): Promise<void> {
  const parent = `projects/reflect-mirror-${stack}`;
  try {
    await secrets.createSecret({
      parent,
      secretId,
      secret: {
        name: secretId,
        replication: {automatic: {}},
      },
    });
  } catch (e) {
    if ((e as {code: GrpcStatus}).code !== GrpcStatus.ALREADY_EXISTS) {
      throw e;
    }
  }

  const secretName = `${parent}/secrets/${secretId}`;
  await secrets.setIamPolicy({
    resource: secretName,
    policy: {
      bindings: [
        {
          role: 'roles/secretmanager.secretAccessor',
          members: [
            `serviceAccount:functions@reflect-mirror-${stack}.iam.gserviceaccount.com`,
          ],
        },
      ],
    },
  });

  try {
    const {payload: latest, version} = await getSecret(stack, secretId);
    if (latest === val) {
      console.log(`Current version of ${secretId} matches specified value`);
      return;
    } else if (!addNextVersion) {
      console.log(`${secretId} (version: ${version}) already exists`);
      return;
    }
  } catch (e) {
    if ((e as {code: GrpcStatus}).code !== GrpcStatus.NOT_FOUND) {
      throw e;
    }
  }

  const result = await secrets.addSecretVersion({
    parent: secretName,
    payload: {data: new TextEncoder().encode(val)},
  });
  console.log(`Successfully stored secret`, result);
}
