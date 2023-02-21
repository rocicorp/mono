export class TestDurableObjectId implements DurableObjectId {
  readonly name?: string;
  private readonly _objectIDString: string;

  constructor(objectIDString: string, name?: string) {
    this._objectIDString = objectIDString;
    if (name !== undefined) {
      this.name = name;
    }
  }
  toString(): string {
    return this._objectIDString;
  }
  equals(other: DurableObjectId): boolean {
    return this.toString() === other.toString();
  }
}

export class TestDurableObjectStub implements DurableObjectStub {
  readonly id: DurableObjectId;
  readonly objectIDString?: string;
  readonly fetch: InstanceType<typeof Fetcher>['fetch'];
  constructor(
    id: DurableObjectId,
    fetch: InstanceType<typeof Fetcher>['fetch'] = () =>
      Promise.resolve(new Response()),
  ) {
    this.id = id;
    this.objectIDString = id.toString();
    this.fetch = fetch;
  }
}

export async function createTestDurableObjectState(
  objectIDString: string,
): Promise<TestDurableObjectState> {
  const id = new TestDurableObjectId(objectIDString);
  const storage = await getMiniflareDurableObjectStorage(id);
  return new TestDurableObjectState(id, storage);
}

export class TestDurableObjectState implements DurableObjectState {
  readonly id: DurableObjectId;
  readonly storage: DurableObjectStorage;
  constructor(id: DurableObjectId, storage: DurableObjectStorage) {
    this.id = id;
    this.storage = storage;
  }
  waitUntil(_promise: Promise<unknown>): void {
    return;
  }
  blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T> {
    return callback();
  }
}

let objectIDCounter = 0;

export function createTestDurableObjectNamespace(): DurableObjectNamespace {
  return {
    newUniqueId: (_options?: DurableObjectNamespaceNewUniqueIdOptions) =>
      // TODO(fritz) support options
      new TestDurableObjectId('unique-id-' + objectIDCounter++),
    // Note: uses the given name for both the object ID and the name.
    idFromName: (name: string) => new TestDurableObjectId(name, name),
    idFromString: (objectIDString: string) =>
      // Note: doesn't support names.
      new TestDurableObjectId(objectIDString),
    get: (id: DurableObjectId) => new TestDurableObjectStub(id),
  };
}
