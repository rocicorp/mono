import {SecretManagerServiceClient} from '@google-cloud/secret-manager';
import {logger} from 'firebase-functions';
import {projectId} from '../config/index.js';

export function apiTokenName(provider: string): string {
  return `${provider}_api_token`;
}

export type SecretValue = {
  version: string;
  payload: string;
};

export interface Secrets {
  getSecret(name: string, version?: string): Promise<SecretValue>;
  getSecretPayload(name: string, version?: string): Promise<string>;
}

export interface SecretsClient {
  fetchSecret(name: string, version?: string): Promise<SecretValue>;
}

export class SecretsClientImpl implements SecretsClient {
  readonly #client = new SecretManagerServiceClient();

  async fetchSecret(name: string, version = 'latest'): Promise<SecretValue> {
    const resource = `projects/${projectId}/secrets/${name}/versions/${version}`;
    logger.debug(`Fetching secret ${resource}`);
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
    logger.debug(`Fetched secret version ${canonicalVersion} (${path})`);
    return {
      version: canonicalVersion,
      payload: typeof data === 'string' ? data : new TextDecoder().decode(data),
    };
  }
}

function cacheKey(name: string, version: string): string {
  return `${name}@${version}`;
}

/**
 * An in-memory cache of secrets that have already been looked up. This is designed
 * to be a _request-level_ cache and not a global one. In particular:
 *
 * - requests for aliased secrets like "version=latest" may resolve to different actual
 *   versions over time
 * - errors, including transient ones, are cached
 *
 * so a SecretsCache instance should only be scoped to a single request.
 */
export class SecretsCache implements Secrets {
  readonly #client: SecretsClient;
  readonly #cache = new Map<string, Promise<SecretValue>>();

  constructor(client: SecretsClient) {
    this.#client = client;
  }

  async getSecret(name: string, version = 'latest'): Promise<SecretValue> {
    const key = cacheKey(name, version);
    let promise = this.#cache.get(key);
    if (!promise) {
      promise = this.#client.fetchSecret(name, version);
      this.#cache.set(key, promise);

      const {version: canonicalVersion} = await promise;
      if (canonicalVersion !== version) {
        // e.g. If the 'latest' version alias resolves version '2', stores
        // the promise at 'my-secret@2' in addition to 'my-secret@latest'.
        this.#cache.set(cacheKey(name, canonicalVersion), promise);
      }
    }
    return promise;
  }

  async getSecretPayload(name: string, version = 'latest'): Promise<string> {
    const value = await this.getSecret(name, version);
    return value.payload;
  }
}
