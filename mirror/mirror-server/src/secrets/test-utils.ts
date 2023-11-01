import type {SecretValue, Secrets, SecretsClient} from './index.js';

export class TestSecrets implements Secrets, SecretsClient {
  // eslint-disable-next-line @typescript-eslint/naming-convention
  static readonly LATEST_ALIAS = '3';
  // eslint-disable-next-line @typescript-eslint/naming-convention
  static readonly TEST_KEY = 'Pmf1KOQv-bK5b4FFmI7ct3pvRX8X13ZsfogYqONgXPw';

  readonly #expectations: [name: string, version: string, value: string][] = [];

  constructor(
    ...expectations: [name: string, version: string, value: string][]
  ) {
    this.#expectations.push(...expectations);
  }

  getSecret(name: string, requested = 'latest'): Promise<SecretValue> {
    return this.fetchSecret(name, requested);
  }

  async getSecretPayload(name: string, requested = 'latest'): Promise<string> {
    const value = await this.fetchSecret(name, requested);
    return value.payload;
  }

  fetchSecret(name: string, requested = 'latest'): Promise<SecretValue> {
    for (let i = 0; i < this.#expectations.length; i++) {
      const [n, v, payload] = this.#expectations[i];
      if (n === name && v === requested) {
        this.#expectations.splice(i, 1);
        const version =
          requested === 'latest' ? TestSecrets.LATEST_ALIAS : requested;
        return Promise.resolve({version, payload});
      }
    }
    return Promise.reject(
      new Error(`Unexpected request for secret ${name}@v${requested}`),
    );
  }
}
