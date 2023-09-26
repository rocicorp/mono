import {SecretManagerServiceClient} from '@google-cloud/secret-manager';
import {GrpcStatus} from 'firebase-admin/firestore';

export async function getSecret(stack: string, id: string): Promise<string> {
  const secrets = new SecretManagerServiceClient();
  const name = `projects/reflect-mirror-${stack}/secrets/${id}/versions/latest`;
  const [{payload}] = await secrets.accessSecretVersion({name});
  if (!payload || !payload.data) {
    throw new Error(`No data for ${id} secret`);
  }
  const {data} = payload;
  return typeof data === 'string' ? data : new TextDecoder().decode(data);
}

export async function storeSecret(
  stack: string,
  secretId: string,
  val: string,
): Promise<void> {
  const secrets = new SecretManagerServiceClient();
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
    const latest = await getSecret(stack, secretId);
    if (latest === val) {
      console.log(`Current version of ${secretId} matches specified value`);
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
