// TODO: This should be a test-only thing.
declare global {
  function getMiniflareBindings(): Bindings;
  function getMiniflareDurableObjectStorage(
    id: DurableObjectId,
  ): Promise<DurableObjectStorage>;

  const MINIFLARE: boolean | undefined;
}

export {};
