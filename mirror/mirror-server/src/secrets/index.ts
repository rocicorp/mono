import {SecretManagerServiceClient} from '@google-cloud/secret-manager';
import {projectId} from '../config/index.js';

export type SecretValue = {
  version: string;
  payload: string;
};

export interface Secrets {
  getSecret(name: string, version?: string): Promise<SecretValue>;
}

export class SecretsClient implements Secrets {
  readonly #client = new SecretManagerServiceClient();

  async getSecret(name: string, version = 'latest'): Promise<SecretValue> {
    const resource = `projects/${projectId}/secrets/${name}/versions/${version}`;
    const [{name: path, payload}] = await this.#client.accessSecretVersion({
      name: resource,
    });
    if (!path || !payload || !payload.data) {
      throw new Error(`No data for ${name} secret`);
    }

    // e.g. "projects/246973677105/secrets/default_api_token/versions/1"
    const pathParts = path.split('/');
    const canonicalVersion = pathParts[pathParts.length - 1];
    const {data} = payload;
    return {
      version: canonicalVersion,
      payload: typeof data === 'string' ? data : new TextDecoder().decode(data),
    };
  }
}
