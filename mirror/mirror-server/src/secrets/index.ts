import {SecretManagerServiceClient} from '@google-cloud/secret-manager';
import {logger} from 'firebase-functions';
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

/**
 * An in-memory cache of secrets that have already been looked up. This is designed
 * to be a _request-level_ cache and not a global one. In particular, requests for
 * aliased secrets like "version=latest" may resolve to different actual versions
 * over time, so caching is only suitable at the request scope.
 */
export class SecretsCache implements Secrets {
  readonly #delegate: Secrets;
  readonly #cache = new Map<string, Promise<SecretValue>>();

  constructor(delegate: Secrets) {
    this.#delegate = delegate;
  }

  #cacheKey(name: string, version: string): string {
    return `${name}@${version}`;
  }

  async getSecret(name: string, version = 'latest'): Promise<SecretValue> {
    const key = this.#cacheKey(name, version);
    let promise = this.#cache.get(key);
    if (!promise) {
      promise = this.#delegate.getSecret(name, version);
      this.#cache.set(key, promise);

      const {version: canonicalVersion} = await promise;
      if (canonicalVersion !== version) {
        // e.g. If the 'latest' version alias resolves version '2', stores
        // the promise at 'my-secret@2' in addition to 'my-secret@latest'.
        this.#cache.set(this.#cacheKey(name, canonicalVersion), promise);
      }
    }
    return promise;
  }
}
