// TODO: This should be a test-only thing.
declare global {
  function getMiniflareBindings(): Bindings;
  function getMiniflareDurableObjectStorage(
    id: DurableObjectId,
  ): Promise<DurableObjectStorage>;

  // eslint-disable-next-line @typescript-eslint/naming-convention
  const MINIFLARE: boolean | undefined;
}

export {};
