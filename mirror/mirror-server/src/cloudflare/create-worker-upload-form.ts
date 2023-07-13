// This is taken from workers-sdk/packages/wrangler/
// TODO(arv): Remove thing we don not need.

/* eslint-disable @typescript-eslint/naming-convention */
import {readFileSync} from 'node:fs';

type ApiCredentials =
  | {
      apiToken: string;
    }
  | {
      authKey: string;
      authEmail: string;
    };

/**
 * A Cloudflare account.
 */

export interface CfAccount {
  /**
   * An API token.
   *
   * @link https://api.cloudflare.com/#user-api-tokens-properties
   */
  apiToken: ApiCredentials;
  /**
   * An account ID.
   */
  accountId: string;
}

/**
 * The type of Worker
 */
export type CfScriptFormat = 'modules' | 'service-worker';

/**
 * A module type.
 */
export type CfModuleType =
  | 'esm'
  | 'commonjs'
  | 'compiled-wasm'
  | 'text'
  | 'buffer';

// HACK
type Buffer = never;

/**
 * An imported module.
 */
export interface CfModule {
  /**
   * The module name.
   *
   * @example
   * './src/index.js'
   */
  name: string;
  /**
   * The module content, usually JavaScript or WASM code.
   *
   * @example
   * export default {
   *   async fetch(request) {
   *     return new Response('Ok')
   *   }
   * }
   */
  content: string | Buffer;
  /**
   * The module type.
   *
   * If absent, will default to the main module's type.
   */
  type?: CfModuleType | undefined;
}

/**
 * A map of variable names to values.
 */
export interface CfVars {
  [key: string]: unknown;
}

/**
 * A KV namespace.
 */
export interface CfKvNamespace {
  binding: string;
  id: string;
}

/**
 * A binding to send email.
 */
export interface CfSendEmailBindings {
  name: string;
  destination_address?: string;
  allowed_destination_addresses?: string[];
}

/**
 * A binding to a wasm module (in service-worker format)
 */

export interface CfWasmModuleBindings {
  [key: string]: string;
}

/**
 * A binding to a text blob (in service-worker format)
 */

export interface CfTextBlobBindings {
  [key: string]: string;
}

/**
 * A binding to a browser
 */

export interface CfBrowserBinding {
  binding: string;
}

/**
 * A binding to a data blob (in service-worker format)
 */

export interface CfDataBlobBindings {
  [key: string]: string;
}

/**
 * A Durable Object.
 */
export interface CfDurableObject {
  name: string;
  class_name: string;
  script_name?: string;
  environment?: string;
}

export interface CfQueue {
  binding: string;
  queue_name: string;
}

export interface CfR2Bucket {
  binding: string;
  bucket_name: string;
}

export const D1_BETA_PREFIX = `__D1_BETA__` as const;
export type D1PrefixedBinding = `${typeof D1_BETA_PREFIX}${string}`;

// TODO: figure out if this is duplicated in packages/wrangler/src/config/environment.ts
export interface CfD1Database {
  // For now, all D1 bindings are alpha
  binding: D1PrefixedBinding;
  database_id: string;
  database_name?: string;
  preview_database_id?: string;
  database_internal_env?: string;
  migrations_table?: string;
  migrations_dir?: string;
}

export interface CfConstellation {
  binding: string;
  project_id: string;
}

interface CfService {
  binding: string;
  service: string;
  environment?: string;
}

interface CfAnalyticsEngineDataset {
  binding: string;
  dataset?: string;
}

interface CfDispatchNamespace {
  binding: string;
  namespace: string;
  outbound?: {
    service: string;
    environment?: string;
    parameters?: string[];
  };
}

interface CfMTlsCertificate {
  binding: string;
  certificate_id: string;
}

interface CfLogfwdr {
  schema: string | undefined;
  bindings: CfLogfwdrBinding[];
}

interface CfLogfwdrBinding {
  name: string;
  destination: string;
}

interface CfUnsafeBinding {
  name: string;
  type: string;
}

type CfUnsafeMetadata = Record<string, unknown>;

interface CfUnsafe {
  bindings: CfUnsafeBinding[] | undefined;
  metadata: CfUnsafeMetadata | undefined;
}

export interface CfDurableObjectMigrations {
  old_tag?: string | undefined;
  new_tag: string;
  steps: {
    new_classes?: string[] | undefined;
    renamed_classes?:
      | {
          from: string;
          to: string;
        }[]
      | undefined;
    deleted_classes?: string[] | undefined;
  }[];
}

export interface CfPlacement {
  mode: 'smart';
}

export interface CfTailConsumer {
  service: string;
  environment?: string;
}

/**
 * Options for creating a `CfWorker`.
 */
export interface CfWorkerInit {
  /**
   * The name of the worker.
   */
  name?: string | undefined;
  /**
   * The entrypoint module.
   */
  main: CfModule;
  /**
   * The list of additional modules.
   */
  modules?: CfModule[] | undefined;
  /**
   * All the bindings
   */
  bindings: {
    vars?: CfVars | undefined;
    kv_namespaces?: CfKvNamespace[] | undefined;
    send_email?: CfSendEmailBindings[] | undefined;
    wasm_modules?: CfWasmModuleBindings | undefined;
    text_blobs?: CfTextBlobBindings | undefined;
    browser?: CfBrowserBinding | undefined;
    data_blobs?: CfDataBlobBindings | undefined;
    durable_objects?: {bindings: CfDurableObject[]} | undefined;
    queues?: CfQueue[] | undefined;
    r2_buckets?: CfR2Bucket[] | undefined;
    d1_databases?: CfD1Database[] | undefined;
    constellation?: CfConstellation[] | undefined;
    services?: CfService[] | undefined;
    analytics_engine_datasets?: CfAnalyticsEngineDataset[] | undefined;
    dispatch_namespaces?: CfDispatchNamespace[] | undefined;
    mtls_certificates?: CfMTlsCertificate[] | undefined;
    logfwdr?: CfLogfwdr | undefined;
    unsafe?: CfUnsafe | undefined;
  };
  migrations?: CfDurableObjectMigrations | undefined;
  compatibility_date?: string | undefined;
  compatibility_flags?: string[] | undefined;
  usage_model?: 'bundled' | 'unbound' | undefined;
  keepVars?: boolean | undefined;
  logpush?: boolean | undefined;
  placement?: CfPlacement | undefined;
  tail_consumers?: CfTailConsumer[] | undefined;
}

// export interface CfWorkerContext {
//   env: string | undefined;
//   legacyEnv: boolean | undefined;
//   zone: string | undefined;
//   host: string | undefined;
//   routes: Route[] | undefined;
//   sendMetrics: boolean | undefined;
// }

export function toMimeType(type: CfModuleType): string {
  switch (type) {
    case 'esm':
      return 'application/javascript+module';
    case 'commonjs':
      return 'application/javascript';
    case 'compiled-wasm':
      return 'application/wasm';
    case 'buffer':
      return 'application/octet-stream';
    case 'text':
      return 'text/plain';
    default:
      throw new TypeError('Unsupported module: ' + type);
  }
}

export type WorkerMetadataBinding =
  // If you add any new binding types here, also add it to safeBindings
  // under validateUnsafeBinding in config/validation.ts
  | {type: 'plain_text'; name: string; text: string}
  | {type: 'json'; name: string; json: unknown}
  | {type: 'wasm_module'; name: string; part: string}
  | {type: 'text_blob'; name: string; part: string}
  | {type: 'browser'; name: string}
  | {type: 'data_blob'; name: string; part: string}
  | {type: 'kv_namespace'; name: string; namespace_id: string}
  | {
      type: 'send_email';
      name: string;
      destination_address?: string | undefined;
      allowed_destination_addresses?: string[] | undefined;
    }
  | {
      type: 'durable_object_namespace';
      name: string;
      class_name: string;
      script_name?: string;
      environment?: string;
    }
  | {type: 'queue'; name: string; queue_name: string}
  | {type: 'r2_bucket'; name: string; bucket_name: string}
  | {type: 'd1'; name: string; id: string; internalEnv?: string | undefined}
  | {type: 'constellation'; name: string; project: string}
  | {type: 'service'; name: string; service: string; environment?: string}
  | {type: 'analytics_engine'; name: string; dataset?: string | undefined}
  | {
      type: 'dispatch_namespace';
      name: string;
      namespace: string;
      outbound?: {
        worker: {
          service: string;
          environment?: string | undefined;
        };
        params?: {name: string}[] | undefined;
      };
    }
  | {type: 'mtls_certificate'; name: string; certificate_id: string}
  | {
      type: 'logfwdr';
      name: string;
      destination: string;
    };

export interface WorkerMetadata {
  /** The name of the entry point module. Only exists when the worker is in the ES module format */
  main_module?: string;
  /** The name of the entry point module. Only exists when the worker is in the service-worker format */
  body_part?: string | undefined;
  compatibility_date?: string;
  compatibility_flags?: string[];
  usage_model?: 'bundled' | 'unbound';
  migrations?: CfDurableObjectMigrations | undefined;
  capnp_schema?: string | undefined;
  bindings: WorkerMetadataBinding[];
  keep_bindings?: WorkerMetadataBinding['type'][];
  logpush?: boolean;
  placement?: CfPlacement;
  tail_consumers?: CfTailConsumer[];
  // Allow unsafe.metadata to add arbitary properties at runtime
  [key: string]: unknown;
}

/**
 * Creates a `FormData` upload from a `CfWorkerInit`.
 */
export function createWorkerUploadForm(worker: CfWorkerInit): FormData {
  const formData = new FormData();
  const {
    main,
    bindings,
    migrations,
    usage_model,
    compatibility_date,
    compatibility_flags,
    keepVars,
    logpush,
    placement,
    tail_consumers,
  } = worker;

  let {modules} = worker;

  const metadataBindings: WorkerMetadata['bindings'] = [];

  Object.entries(bindings.vars || {})?.forEach(([key, value]) => {
    if (typeof value === 'string') {
      metadataBindings.push({name: key, type: 'plain_text', text: value});
    } else {
      metadataBindings.push({name: key, type: 'json', json: value});
    }
  });

  bindings.kv_namespaces?.forEach(({id, binding}) => {
    metadataBindings.push({
      name: binding,
      type: 'kv_namespace',
      namespace_id: id,
    });
  });

  bindings.send_email?.forEach(
    ({name, destination_address, allowed_destination_addresses}) => {
      metadataBindings.push({
        name,
        type: 'send_email',
        destination_address,
        allowed_destination_addresses,
      });
    },
  );

  bindings.durable_objects?.bindings.forEach(
    ({name, class_name, script_name, environment}) => {
      metadataBindings.push({
        name,
        type: 'durable_object_namespace',
        class_name,
        ...(script_name && {script_name}),
        ...(environment && {environment}),
      });
    },
  );

  bindings.queues?.forEach(({binding, queue_name}) => {
    metadataBindings.push({
      type: 'queue',
      name: binding,
      queue_name,
    });
  });

  bindings.r2_buckets?.forEach(({binding, bucket_name}) => {
    metadataBindings.push({
      name: binding,
      type: 'r2_bucket',
      bucket_name,
    });
  });

  bindings.d1_databases?.forEach(
    ({binding, database_id, database_internal_env}) => {
      metadataBindings.push({
        name: binding,
        type: 'd1',
        id: database_id,
        internalEnv: database_internal_env,
      });
    },
  );

  bindings.constellation?.forEach(({binding, project_id}) => {
    metadataBindings.push({
      name: binding,
      type: 'constellation',
      project: project_id,
    });
  });

  bindings.services?.forEach(({binding, service, environment}) => {
    metadataBindings.push({
      name: binding,
      type: 'service',
      service,
      ...(environment && {environment}),
    });
  });

  bindings.analytics_engine_datasets?.forEach(({binding, dataset}) => {
    metadataBindings.push({
      name: binding,
      type: 'analytics_engine',
      dataset,
    });
  });

  bindings.dispatch_namespaces?.forEach(({binding, namespace, outbound}) => {
    metadataBindings.push({
      name: binding,
      type: 'dispatch_namespace',
      namespace,
      ...(outbound && {
        outbound: {
          worker: {
            service: outbound.service,
            environment: outbound.environment,
          },
          params: outbound.parameters?.map(p => ({name: p})),
        },
      }),
    });
  });

  bindings.mtls_certificates?.forEach(({binding, certificate_id}) => {
    metadataBindings.push({
      name: binding,
      type: 'mtls_certificate',
      certificate_id,
    });
  });

  bindings.logfwdr?.bindings.forEach(({name, destination}) => {
    metadataBindings.push({
      name,
      type: 'logfwdr',
      destination,
    });
  });

  for (const [name, filePath] of Object.entries(bindings.wasm_modules || {})) {
    metadataBindings.push({
      name,
      type: 'wasm_module',
      part: name,
    });

    formData.set(
      name,
      new File([readFileSync(filePath)], filePath, {
        type: 'application/wasm',
      }),
    );
  }

  if (bindings.browser !== undefined) {
    metadataBindings.push({
      name: bindings.browser.binding,
      type: 'browser',
    });
  }

  for (const [name, filePath] of Object.entries(bindings.text_blobs || {})) {
    metadataBindings.push({
      name,
      type: 'text_blob',
      part: name,
    });

    if (name !== '__STATIC_CONTENT_MANIFEST') {
      formData.set(
        name,
        new File([readFileSync(filePath)], filePath, {
          type: 'text/plain',
        }),
      );
    }
  }

  for (const [name, filePath] of Object.entries(bindings.data_blobs || {})) {
    metadataBindings.push({
      name,
      type: 'data_blob',
      part: name,
    });

    formData.set(
      name,
      new File([readFileSync(filePath)], filePath, {
        type: 'application/octet-stream',
      }),
    );
  }

  if (main.type === 'commonjs') {
    // This is a service-worker format worker.
    for (const module of Object.values([...(modules || [])])) {
      if (module.name === '__STATIC_CONTENT_MANIFEST') {
        // Add the manifest to the form data.
        formData.set(
          module.name,
          new File([module.content], module.name, {
            type: 'text/plain',
          }),
        );
        // And then remove it from the modules collection
        modules = modules?.filter(m => m !== module);
      } else if (
        module.type === 'compiled-wasm' ||
        module.type === 'text' ||
        module.type === 'buffer'
      ) {
        // Convert all wasm/text/data modules into `wasm_module`/`text_blob`/`data_blob` bindings.
        // The "name" of the module is a file path. We use it
        // to instead be a "part" of the body, and a reference
        // that we can use inside our source. This identifier has to be a valid
        // JS identifier, so we replace all non alphanumeric characters
        // with an underscore.
        const name = module.name.replace(/[^a-zA-Z0-9_$]/g, '_');
        metadataBindings.push({
          name,
          type:
            module.type === 'compiled-wasm'
              ? 'wasm_module'
              : module.type === 'text'
              ? 'text_blob'
              : 'data_blob',
          part: name,
        });

        // Add the module to the form data.
        formData.set(
          name,
          new File([module.content], module.name, {
            type:
              module.type === 'compiled-wasm'
                ? 'application/wasm'
                : module.type === 'text'
                ? 'text/plain'
                : 'application/octet-stream',
          }),
        );
        // And then remove it from the modules collection
        modules = modules?.filter(m => m !== module);
      }
    }
  }

  if (bindings.unsafe?.bindings) {
    // @ts-expect-error unsafe bindings don't need to match a specific type here
    metadataBindings.push(...bindings.unsafe.bindings);
  }

  const metadata: WorkerMetadata = {
    ...(main.type !== 'commonjs'
      ? {main_module: main.name}
      : {body_part: main.name}),
    bindings: metadataBindings,
    ...(compatibility_date && {compatibility_date}),
    ...(compatibility_flags && {compatibility_flags}),
    ...(usage_model && {usage_model}),
    ...(migrations && {migrations}),
    capnp_schema: bindings.logfwdr?.schema,
    ...(keepVars && {keep_bindings: ['plain_text', 'json']}),
    ...(logpush !== undefined && {logpush}),
    ...(placement && {placement}),
    ...(tail_consumers && {tail_consumers}),
  };

  if (bindings.unsafe?.metadata !== undefined) {
    for (const key of Object.keys(bindings.unsafe.metadata)) {
      metadata[key] = bindings.unsafe.metadata[key];
    }
  }

  formData.set('metadata', JSON.stringify(metadata));

  if (main.type === 'commonjs' && modules && modules.length > 0) {
    throw new TypeError(
      "More than one module can only be specified when type = 'esm'",
    );
  }

  for (const module of [main].concat(modules || [])) {
    formData.set(
      module.name,
      new Blob([module.content], {
        type: toMimeType(module.type ?? main.type ?? 'esm'),
      }),
      module.name,
    );
  }

  if (bindings.logfwdr && bindings.logfwdr.schema) {
    const filePath = bindings.logfwdr.schema;
    formData.set(
      filePath,
      new Blob([readFileSync(filePath)], {
        type: 'application/octet-stream',
      }),
      filePath,
    );
  }

  return formData;
}
