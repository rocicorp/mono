import type {Storage} from 'firebase-admin/storage';
import {
  Script,
  NamespacedScript,
  GlobalScript,
  NamespacedName,
} from 'cloudflare-api/src/scripts.js';
import {Errors, FetchResultError} from 'cloudflare-api/src/fetch.js';
import type {ZoneConfig} from './config.js';
import {
  deleteCustomHostnames,
  publishCustomHostname,
} from './publish-custom-hostnames.js';
import {logger} from 'firebase-functions';
import type {
  DeploymentOptions,
  DeploymentSecrets,
} from 'mirror-schema/src/deployment.js';
import type {ModuleRef} from 'mirror-schema/src/module.js';
import {ModuleAssembler} from './module-assembler.js';
import type {CfModule} from 'cloudflare-api/src/create-script-upload-form.js';
import {uploadScript} from './publish.js';
import {submitSecret} from './submit-secret.js';
import {publishCustomDomains} from './publish-custom-domains.js';
import {submitTriggers} from './submit-triggers.js';
import type {AccountAccess} from 'cloudflare-api/src/resources.js';
import {Resolver} from 'dns/promises';
import {HttpsError} from 'firebase-functions/v2/https';
import {sleep} from 'shared/src/sleep.js';

export type ScriptHandler = {
  publish(
    storage: Storage,
    app: {id: string; name: string},
    team: {id: string; label: string},
    hostname: string,
    options: DeploymentOptions,
    secrets: DeploymentSecrets,
    appModules: ModuleRef[],
    serverModules: ModuleRef[],
  ): AsyncGenerator<string>;

  delete(): Promise<void>;
};

abstract class AbstractScriptHandler<S extends Script = Script>
  implements ScriptHandler
{
  protected readonly _script: S;
  protected readonly _zone: ZoneConfig;

  constructor(script: S, zone: ZoneConfig) {
    this._script = script;
    this._zone = zone;
  }

  async *publish(
    storage: Storage,
    app: {id: string; name: string},
    team: {id: string; label: string},
    hostname: string,
    options: DeploymentOptions,
    secrets: DeploymentSecrets,
    appModules: ModuleRef[],
    serverModules: ModuleRef[],
  ): AsyncGenerator<string> {
    logger.log(`Publishing ${hostname} (${this._script.id})`);

    const assembler = new ModuleAssembler(
      app.name,
      team.label,
      this._script.id,
      appModules,
      serverModules,
    );
    const tags = [
      `appID:${app.id}`,
      `appName:${app.name}`,
      `teamID:${team.id}`,
      `teamLabel:${team.label}`,
    ];

    for await (const msg of this._doPublish(
      assembler.assemble(storage),
      hostname,
      options,
      secrets,
      tags,
    )) {
      yield msg;
    }
    for await (const msg of waitForLiveness(hostname)) {
      yield msg;
    }
  }

  protected abstract _doPublish(
    assembled: Promise<CfModule[]>,
    hostname: string,
    options: DeploymentOptions,
    secrets: DeploymentSecrets,
    tags: string[],
  ): AsyncGenerator<string>;

  async delete(): Promise<void> {
    try {
      await this._script.delete(new URLSearchParams({force: 'true'}));
    } catch (e) {
      FetchResultError.throwIfCodeIsNot(
        e,
        Errors.ScriptNotFound,
        Errors.CouldNotRouteToScript,
      );
    }
  }
}

export class GlobalScriptHandler extends AbstractScriptHandler<GlobalScript> {
  constructor(account: AccountAccess, zone: ZoneConfig, name: string) {
    super(new GlobalScript(account, name), zone);
  }

  // eslint-disable-next-line require-yield
  protected async *_doPublish(
    assembled: Promise<CfModule[]>,
    hostname: string,
    options: DeploymentOptions,
    secrets: DeploymentSecrets,
    tags: string[],
  ): AsyncGenerator<string> {
    const modules = await assembled;
    await uploadScript(
      this._script,
      modules[0],
      modules.slice(1),
      options.vars,
      tags,
    );

    await Promise.all([
      publishCustomDomains(this._script, hostname),
      submitTriggers(this._script, '*/5 * * * *'),
      ...Object.entries(secrets).map(([name, value]) =>
        submitSecret(this._script, name, value),
      ),
    ]);
  }
}

export class NamespacedScriptHandler extends AbstractScriptHandler<NamespacedScript> {
  constructor(account: AccountAccess, zone: ZoneConfig, name: NamespacedName) {
    super(new NamespacedScript(account, name), zone);
  }

  protected async *_doPublish(
    assembled: Promise<CfModule[]>,
    hostname: string,
    options: DeploymentOptions,
    secrets: DeploymentSecrets,
    tags: string[],
  ): AsyncGenerator<string> {
    const published = assembled
      .then(modules =>
        uploadScript(
          this._script,
          modules[0],
          modules.slice(1),
          options.vars,
          tags,
        ),
      )
      .then(() =>
        Promise.all(
          Object.entries(secrets).map(([name, value]) =>
            submitSecret(this._script, name, value),
          ),
        ),
      );

    // Custom Hostnames are not explicitly tied to scripts, so they can be set up / managed
    // in parallel to the deployment of the script.
    for await (const msg of publishCustomHostname(
      this._zone,
      this._script,
      hostname,
    )) {
      yield msg;
    }

    await published;
  }

  async delete(): Promise<void> {
    await Promise.all([
      super.delete(),
      await deleteCustomHostnames(this._zone, this._script),
    ]);
  }
}

// Checks Cloudflare's DNS servers to determine when the hostname resolves to an IP
// address, and then connects to the IP address via SSL and a Host header to ensure
// that the request is routed properly.
//
// Note that the liveness check explicitly avoids the `getaddrinfo` syscall as that
// has a tendency to cache negative lookups (ENOTFOUND) indefinitely. In particular,
// we use dns.Resolver.resolve() instead of lookup, and we have fetch connect directly
// to the resulting IP address (to avoid fetch calling `getaddrinfo` to resolve the
// hostname).
const DNS_POLL_INTERVAL = 3000;
const FETCH_POLL_INTERVAL = 1000;
const LIVENESS_TIMEOUT = 3 * 60 * 1000;
const CLOUDFLARE_DNS_SERVERS = ['1.1.1.1'] as const;

export async function* waitForLiveness(host: string): AsyncGenerator<string> {
  const resolver = new Resolver();
  resolver.setServers(CLOUDFLARE_DNS_SERVERS);

  const start = Date.now();
  let ip: string;
  for (let first = true; ; first = false) {
    try {
      const res = await resolver.resolve4(host);
      logger.info(`${host} resolves to ${res}`);
      ip = res[0];
      break;
    } catch (err) {
      logger.debug(`resolve(${host}) DNS error`, err);
    }
    if (first) {
      yield `Waiting for DNS to resolve`;
    }
    if (Date.now() - start > LIVENESS_TIMEOUT) {
      throw new HttpsError(
        'deadline-exceeded',
        `Timed out waiting for ${host}. DNS records may not be correctly set up.`,
      );
    }
    await sleep(DNS_POLL_INTERVAL);
  }

  yield `Verifying liveness of https://${host}/`;
  const url = `https://${ip}:443/`;
  for (;;) {
    try {
      const res = await fetch(url, {headers: {host}});
      if (res.ok) {
        logger.info(`${url} (Host: ${host}) is live`);
        break;
      }
      logger.debug(`GET ${url}: ${res.status}`);
    } catch (err) {
      logger.debug(`GET ${url} error`, err);
    }
    if (Date.now() - start > LIVENESS_TIMEOUT) {
      throw new HttpsError(
        'deadline-exceeded',
        `Timed out waiting for https://${host}/`,
      );
    }
    await sleep(FETCH_POLL_INTERVAL);
  }
}
