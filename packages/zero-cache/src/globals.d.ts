// TODO: This should be a test-only thing.
import {
  DurableObjectId,
  DurableObjectNamespace,
  DurableObjectStorage,
} from '@cloudflare/workers-types';

interface Bindings {
  runnerDO: DurableObjectNamespace;
}

declare global {
  function getMiniflareBindings(): Bindings;
  function getMiniflareDurableObjectStorage(
    id: DurableObjectId,
  ): Promise<DurableObjectStorage>;

  // eslint-disable-next-line @typescript-eslint/naming-convention
  const MINIFLARE: boolean | undefined;
}

export {};
