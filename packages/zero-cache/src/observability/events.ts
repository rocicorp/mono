import type {LogContext} from '@rocicorp/logger';
import {CloudEvent, emitterFor, httpTransport} from 'cloudevents';
import {nanoid} from 'nanoid';
import {isJSONValue, type JSONObject} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import * as v from '../../../shared/src/valita.ts';
import {type ZeroEvent} from '../../../zero-events/src/index.ts';
import type {NormalizedZeroConfig} from '../config/normalize.ts';

const MAX_PUBLISH_ATTEMPTS = 6;
const INITIAL_PUBLISH_BACKOFF_MS = 500;

type PublisherFn = (lc: LogContext, event: ZeroEvent) => Promise<void>;

let publishFn: PublisherFn = (lc, {type}) => {
  lc.warn?.(
    `Cannot publish "${type}" event before initEventSink(). ` +
      `This is only expected in unit tests.`,
  );
  return promiseVoid;
};

const attributeValueSchema = v.union(v.string(), v.number(), v.boolean());

const eventSchema = v.record(attributeValueSchema);

type PartialEvent = v.Infer<typeof eventSchema>;

// Note: This conforms to the format of the knative K_CE_OVERRIDES binding:
// https://github.com/knative/eventing/blob/main/docs/spec/sources.md#sinkbinding
const extensionsObjectSchema = v.object({extensions: eventSchema});

/**
 * Initializes a per-process event sink according to the cloud event
 * parameters in the ZeroConfig. This must be called at the beginning
 * of the process, before any ZeroEvents are generated / published.
 */
export function initEventSink(
  lc: LogContext,
  {taskID, cloudEvent}: Pick<NormalizedZeroConfig, 'taskID' | 'cloudEvent'>,
) {
  if (!cloudEvent.sinkEnv) {
    // The default implementation just outputs the events to logs.
    publishFn = (lc, event) => {
      lc.info?.(`ZeroEvent: ${event.type}`, event);
      return promiseVoid;
    };
    return;
  }

  let overrides: PartialEvent = {};

  if (cloudEvent.extensionOverridesEnv) {
    const strVal = must(process.env[cloudEvent.extensionOverridesEnv]);
    const {extensions} = v.parse(JSON.parse(strVal), extensionsObjectSchema);
    overrides = extensions;
  }

  function createCloudEvent(data: ZeroEvent) {
    const {type, time} = data;
    return new CloudEvent({
      id: nanoid(),
      source: taskID,
      type,
      time,
      data,
      ...overrides,
    });
  }

  const sinkURI = must(process.env[cloudEvent.sinkEnv]);
  const emit = emitterFor(httpTransport(sinkURI));
  lc.debug?.(`Publishing ZeroEvents to ${sinkURI}`);

  publishFn = async (lc, event) => {
    const cloudEvent = createCloudEvent(event);
    lc.debug?.(`Publishing CloudEvent: ${cloudEvent.type}`);

    for (let i = 0; i < MAX_PUBLISH_ATTEMPTS; i++) {
      if (i > 0) {
        // exponential backoff on retries
        await sleep(INITIAL_PUBLISH_BACKOFF_MS * 2 ** (i - 1));
      }
      try {
        await emit(cloudEvent);
        lc.info?.(`Published CloudEvent: ${cloudEvent.type}`, cloudEvent);
        return;
      } catch (e) {
        lc.warn?.(`Error publishing ${cloudEvent.type} (attempt ${i + 1})`, e);
      }
    }
  };
}

export function initEventSinkForTesting(sink: ZeroEvent[], now = new Date()) {
  publishFn = (lc, event) => {
    lc.info?.(`Testing event sink received ${event.type} event`, event);
    // Replace the default Date.now() with the test instance for determinism.
    sink.push({...event, time: now.toISOString()});
    return promiseVoid;
  };
}

export function publishEvent<E extends ZeroEvent>(lc: LogContext, event: E) {
  void publishFn(lc, event);
}

export async function publishCriticalEvent<E extends ZeroEvent>(
  lc: LogContext,
  event: E,
) {
  await publishFn(lc, event);
}

export function makeErrorDetails(e: unknown): JSONObject {
  const err = e instanceof Error ? e : new Error(String(e));
  const errorDetails: JSONObject = {
    name: err.name,
    message: err.message,
    stack: err.stack,
    cause: err.cause ? makeErrorDetails(err.cause) : undefined,
  };
  // Include any enumerable properties (e.g. of Error subtypes).
  for (const [field, value] of Object.entries(err)) {
    if (isJSONValue(value, [])) {
      errorDetails[field] = value;
    }
  }
  return errorDetails;
}
