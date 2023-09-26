import {
  DeleteOnlyFn,
  GetOnlyFn,
  RawSetOnlyFn,
  Resource,
  SetOnlyFn,
} from './resources.js';

/* eslint-disable @typescript-eslint/naming-convention */
export type Binding = {
  type: string;
  name: string;
  namespace_id: string;
};

export type Migration = {
  new_tag: string;
  old_tag: string;
  deleted_classes?: string[];
  new_classes?: string[];
  renamed_classes?: {from: string; to: string}[];
  transferred_classes?: {from: string; from_script: string; to: string}[];
};

export type Placement = {
  mode?: string;
};

export type TailConsumer = {
  environment: string;
  namespace: string;
  service: string;
};

export type UsageModel = 'bundled' | 'unbound';

// https://developers.cloudflare.com/api/operations/namespace-worker-get-script-settings
export type ScriptSettings = {
  bindings: Binding[];
  compatibility_date: string;
  compatibility_flags: string[];
  logpush: boolean;
  migrations: Migration[];
  placement: Placement;
  tags: string[];
  tail_consumers: TailConsumer[];
  usage_model: UsageModel;
};

export type ScriptState = {
  created_on: string;
  modified_on: string;
  id: string;
  tag: string;
  tags: string[];
  deployment_id: string;
  tail_consumers: TailConsumer[] | null;
  logpush: boolean;
  etag: string;
  handlers: string[];
  last_deployed_from: string;
  migration_tag?: string;
  compatibility_date: string;
  compatibility_flags?: string[];
  usage_module: UsageModel;
};

export type ScriptEnvironment = {
  environment?: string;
  dispatch_namespace?: string;
  created_on: string;
  modified_on: string;
  script: ScriptState;
};

export type ScriptSecret = {
  name: string;
  text: string;
  type: 'secret_text';
};

export type ScriptSchedule = {
  cron: string;
};

export type CustomDomains = {
  override_scope: boolean;
  override_existing_origin: boolean;
  override_existing_dns_record: boolean;
  origins: {hostname: string}[];
};
/* eslint-enable @typescript-eslint/naming-convention */

/**
 * A reference to a Worker Script that can represent either a traditional
 * "global" script or a Workers for Platforms namespaced script.
 */
export abstract class Script {
  // An unique ID that is suitable for describing the Script. This does not
  // necessarily correspond to any Cloudflare-specific ID.
  abstract readonly id: string;
  abstract readonly productionEnvironment: GetOnlyFn<ScriptEnvironment>;

  protected readonly _script: Resource;

  readonly name: string;
  readonly upload: RawSetOnlyFn<FormData, {id: string}>;
  readonly settings: GetOnlyFn<ScriptSettings>;
  readonly putSecret: SetOnlyFn<ScriptSecret>;
  readonly delete: DeleteOnlyFn;

  constructor(resource: Resource, name: string) {
    this._script = resource.append(name);
    this.name = name;
    this.upload = this._script.rawPut;
    this.settings = this._script.append('settings').get;
    this.putSecret = this._script.append('secrets').put;
    this.delete = this._script.delete;
  }
}

export class NamespacedScript extends Script {
  readonly id: string;
  readonly namespace: string;
  readonly productionEnvironment: GetOnlyFn<ScriptEnvironment>;

  constructor(
    apiToken: string,
    accountID: string,
    namespace: string,
    name: string,
  ) {
    super(
      new Resource(
        apiToken,
        `/accounts/${accountID}/workers/dispatch/namespaces/${namespace}/scripts`,
      ),
      name,
    );

    this.id = `${namespace}/${name}`;
    this.namespace = namespace;
    this.productionEnvironment = this._script.get;
  }
}

export class GlobalScript extends Script {
  readonly id: string;
  readonly productionEnvironment: GetOnlyFn<ScriptEnvironment>;
  readonly setSchedules: SetOnlyFn<ScriptSchedule[]>;
  readonly setCustomDomains: SetOnlyFn<CustomDomains>;

  constructor(apiToken: string, accountID: string, name: string) {
    super(
      new Resource(apiToken, `/accounts/${accountID}/workers/scripts`),
      name,
    );

    const service = new Resource(
      apiToken,
      `/accounts/${accountID}/workers/services`,
    ).append(name);

    this.id = name;
    this.productionEnvironment = service.append(`environments/production`).get;
    this.setSchedules = this._script.append('schedules').put;
    this.setCustomDomains = this._script.append('domains/records').put;
  }
}
