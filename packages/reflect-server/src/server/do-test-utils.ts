export class TestDurableObjectId implements DurableObjectId {
  readonly name: string;
  constructor(name: string) {
    this.name = name;
  }
  toString(): string {
    return this.name;
  }
  equals(other: DurableObjectId): boolean {
    return this.name === other.name;
  }
}

export class TestDurableObjectStub implements DurableObjectStub {
  readonly id: DurableObjectId;
  readonly name?: string;
  readonly fetch: InstanceType<typeof Fetcher>["fetch"];
  constructor(
    id: DurableObjectId,
    fetch: InstanceType<typeof Fetcher>["fetch"] = () => {
      return Promise.resolve(new Response());
    }
  ) {
    this.id = id;
    this.name = id.name;
    this.fetch = fetch;
  }
}

export async function createTestDurableObjectState(
  name: string
): Promise<TestDurableObjectState> {
  const id = new TestDurableObjectId(name);
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

export function createTestDurableObjectNamespace(): DurableObjectNamespace {
  return {
    newUniqueId: (_options?: DurableObjectNamespaceNewUniqueIdOptions) => {
      throw new Error(
        "TestDurableObjectNamespace does not yet support newUniqueId"
      );
    },
    idFromName: (name: string) => {
      return new TestDurableObjectId(name);
    },
    idFromString: (_id: string) => {
      throw new Error(
        "TestDurableObjectNamespace does not yet support idFromString"
      );
    },
    get: (id: DurableObjectId) => {
      return new TestDurableObjectStub(id);
    },
  };
}
