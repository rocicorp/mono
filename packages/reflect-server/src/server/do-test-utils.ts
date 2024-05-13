import {assert} from 'shared/src/asserts.js';

export type IncomingRequest = Request<
  unknown,
  IncomingRequestCfProperties<unknown>
>;

export class TestExecutionContext implements ExecutionContext {
  waitUntil(_promise: Promise<unknown>): void {
    return;
  }
  passThroughOnException(): void {
    return;
  }
}

export class TestDurableObjectId implements DurableObjectId {
  readonly name?: string;
  readonly #objectIDString: string;

  constructor(objectIDString: string, name?: string) {
    this.#objectIDString = objectIDString;
    if (name !== undefined) {
      this.name = name;
    }
  }
  toString(): string {
    return this.#objectIDString;
  }
  equals(other: DurableObjectId): boolean {
    return this.toString() === other.toString();
  }
}

export class TestDurableObjectStub implements DurableObjectStub {
  readonly id: DurableObjectId;
  readonly objectIDString?: string;
  readonly fetch: DurableObjectStub['fetch'];
  constructor(
    id: DurableObjectId,
    fetch: DurableObjectStub['fetch'] = () => Promise.resolve(new Response()),
  ) {
    this.id = id;
    this.objectIDString = id.toString();
    this.fetch = (
      requestOrUrl: Request | string,
      requestInit?: RequestInit | Request,
    ) => {
      if (requestOrUrl instanceof Request) {
        assert(
          !requestOrUrl.bodyUsed,
          'Body of request passed to TestDurableObjectStub fetch already used.',
        );
      }
      if (requestInit instanceof Request) {
        assert(
          !requestInit.bodyUsed,
          'Body of request passed to TestDurableObjectStub fetch already used.',
        );
      }
      return fetch(requestOrUrl, requestInit);
    };
  }
  connect(
    _address: string | SocketAddress,
    _options?: SocketOptions | undefined,
  ): Socket {
    throw new Error('Method not implemented.');
  }
  name?: string;
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
  readonly #blockingCallbacks: Promise<unknown>[] = [];

  constructor(id: DurableObjectId, storage: DurableObjectStorage) {
    this.id = id;
    this.storage = storage;
  }
  waitUntil(_promise: Promise<unknown>): void {
    return;
  }
  blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T> {
    const promise = callback();
    this.#blockingCallbacks.push(promise);
    return promise;
  }
  concurrencyBlockingCallbacks(): Promise<unknown[]> {
    return Promise.all(this.#blockingCallbacks);
  }
  acceptWebSocket(_ws: WebSocket, _tags?: string[] | undefined): void {
    throw new Error('Method not implemented.');
  }
  getWebSockets(_tag?: string | undefined): WebSocket[] {
    throw new Error('Method not implemented.');
  }
  setWebSocketAutoResponse(
    _maybeReqResp?: WebSocketRequestResponsePair | undefined,
  ): void {
    throw new Error('Method not implemented.');
  }
  getWebSocketAutoResponse(): WebSocketRequestResponsePair | null {
    throw new Error('Method not implemented.');
  }
  getWebSocketAutoResponseTimestamp(_ws: WebSocket): Date | null {
    throw new Error('Method not implemented.');
  }
  setHibernatableWebSocketEventTimeout(_timeoutMs?: number | undefined): void {
    throw new Error('Method not implemented.');
  }
  getHibernatableWebSocketEventTimeout(): number | null {
    throw new Error('Method not implemented.');
  }
  getTags(_ws: WebSocket): string[] {
    throw new Error('Method not implemented.');
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
    jurisdiction: (_jurisdiction: DurableObjectJurisdiction) =>
      createTestDurableObjectNamespace(),
  };
}
