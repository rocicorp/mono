import {OptionalLogger} from '@rocicorp/logger';
import {
  FirstKeyFunc,
  IDFromEntityFunc,
  KeyFromEntityFunc,
  KeyFromLookupIDFunc,
  KeyToIDFunc,
  ListOptionsWith,
  Parse,
  ParseInternal,
  ReadTransaction,
  WriteTransaction,
  deleteImpl,
  getImpl,
  hasImpl,
  initImpl,
  listEntriesImpl,
  listIDsImpl,
  listImpl,
  maybeParse,
  mustGetImpl,
  scan,
  setImpl,
  updateImpl,
} from './generate.js';

/**
 * For presence entities there are two common cases:
 * 1. The entity does not have an `id` field. Then there can only be one entity
 *    per client. This case is useful for keeping track of things like the
 *    cursor position.
 * 2. The entity has an `id` field. Then there can be multiple entities per
 *    client. This case is useful for keeping track of things like multiple
 *    selection or multiple cursors (aka multi touch).
 */
export type PresenceEntity = {
  clientID: string;
  id?: string;
};

type IsIDMissing<T> = 'id' extends keyof T ? false : true;

/**
 * Like {@link PresenceEntity}, but with the clientID optional. This is used
 * when doing get, has and delete operations where the clientID field defaults
 * to the current client.
 */
export type PresenceID<T extends PresenceEntity> =
  IsIDMissing<T> extends false
    ? {
        clientID?: string;
        id: string;
      }
    : {
        clientID?: string;
      };

export type StartAtID<T extends PresenceEntity> =
  IsIDMissing<T> extends true ? {clientID: string} : PresenceEntity;

type ListID<T extends PresenceEntity> =
  IsIDMissing<T> extends true
    ? {clientID: string}
    : undefined extends T['id']
      ? PresenceEntity
      : {clientID: string; id: string};

/**
 * When mutating an entity, you can omit the `clientID`. This type marks that
 * field as optional.
 */
export type OptionalClientID<T extends PresenceEntity> = {
  clientID?: string | undefined;
} & Omit<T, 'clientID'>;

export type PresenceListOptions<T extends PresenceEntity> = {
  startAtID?: StartAtID<T>;
  limit?: number;
};

export type PresenceUpdate<T extends PresenceEntity> =
  IsIDMissing<T> extends false ? Pick<T, 'id'> & Partial<T> : Partial<T>;

export type GeneratePresenceResult<T extends PresenceEntity> = {
  /** Write `value`, overwriting any previous version of same value. */
  set: (tx: WriteTransaction, value: OptionalClientID<T>) => Promise<void>;
  /**
   * Write `value`, overwriting any previous version of same value.
   * @deprecated Use `set` instead.
   */
  put: (tx: WriteTransaction, value: OptionalClientID<T>) => Promise<void>;
  /** Write `value` only if no previous version of this value exists. */
  init: (tx: WriteTransaction, value: OptionalClientID<T>) => Promise<boolean>;
  /** Update existing value with new fields. */
  update: (tx: WriteTransaction, value: PresenceUpdate<T>) => Promise<void>;
  /** Delete any existing value or do nothing if none exist. */
  delete: (tx: WriteTransaction, id?: PresenceID<T>) => Promise<void>;
  /** Return true if specified value exists, false otherwise. */
  has: (tx: ReadTransaction, id?: PresenceID<T>) => Promise<boolean>;
  /** Get value by ID, or return undefined if none exists. */
  get: (tx: ReadTransaction, id?: PresenceID<T>) => Promise<T | undefined>;
  /** Get value by ID, or throw if none exists. */
  mustGet: (tx: ReadTransaction, id?: PresenceID<T>) => Promise<T>;
  /** List values matching criteria. */
  list: (tx: ReadTransaction, options?: PresenceListOptions<T>) => Promise<T[]>;

  /**
   * List IDs matching criteria. The returned ID is `{clientID: string}` if the
   * entry has no `id` field, otherwise it is `{clientID: string, id: string}`.
   */
  listIDs: (
    tx: ReadTransaction,
    options?: PresenceListOptions<T>,
  ) => Promise<ListID<T>[]>;

  /**
   * List `clientID`s matching criteria. Unlike `listIDs` this returns an array
   * of strings consisting of the `clientID`s.
   */
  listClientIDs: (
    tx: ReadTransaction,
    options?: PresenceListOptions<T>,
  ) => Promise<string[]>;

  /** List [id, value] entries matching criteria. */
  listEntries: (
    tx: ReadTransaction,
    options?: PresenceListOptions<T>,
  ) => Promise<[ListID<T>, T][]>;
};

const presencePrefix = '-/p/';

export function keyFromID(name: string, entity: PresenceEntity) {
  const {clientID, id} = entity;
  if (id !== undefined) {
    return `${presencePrefix}${clientID}/${name}/id/${id}`;
  }
  return `${presencePrefix}${clientID}/${name}/`;
}

export function parseKeyToID(
  name: string,
  key: string,
): {clientID: string} | {clientID: string; id: string} | undefined {
  const parts = key.split('/');
  if (
    parts.length < 5 ||
    parts[0] !== '-' ||
    parts[1] !== 'p' ||
    parts[2] === '' || // clientID
    parts[3] !== name ||
    (parts[4] !== 'id' && parts[4] !== '')
  ) {
    return undefined;
  }

  // Now we know the key starts with '-/p/{clientID}/name/' or '-/p/{clientID}/name/id/{id}'
  if (parts.length === 5 && parts[4] === '') {
    return {clientID: parts[2]};
  }
  if (parts.length === 6 && parts[4] === 'id') {
    return {clientID: parts[2], id: parts[5]};
  }

  return undefined;
}

const idFromEntity: IDFromEntityFunc<PresenceEntity, PresenceEntity> = (
  _tx: ReadTransaction,
  entity: PresenceEntity,
) =>
  entity.id === undefined
    ? {clientID: entity.clientID}
    : {clientID: entity.clientID, id: entity.id};

function normalizePresenceID<T extends PresenceEntity>(
  tx: {clientID: string},
  base: Partial<PresenceEntity> | undefined,
): ListID<T> {
  // When we replay mutations (delete in this case) undefined arguments gets converted to null.
  //
  //   deleteEntity()
  //
  // becomes:
  //
  //   deleteEntity(null)
  //
  // when rebasing.
  // eslint-disable-next-line eqeqeq
  if (base == null) {
    return {clientID: tx.clientID} as ListID<T>;
  }
  const {clientID = tx.clientID, id} = base;
  return (id === undefined ? {clientID} : {clientID, id}) as ListID<T>;
}

function normalizeForUpdate<T extends PresenceEntity>(
  tx: {clientID: string},
  v: PresenceUpdate<T>,
): PresenceUpdate<T> & {clientID: string} {
  return normalizeForSet(tx, v);
}

function normalizeForSet<
  T extends PresenceEntity,
  V extends OptionalClientID<T>,
>(tx: {clientID: string}, v: V): V & {clientID: string} {
  if (v === null) {
    throw new TypeError('Expected object, received null');
  }
  if (typeof v !== 'object') {
    throw new TypeError(`Expected object, received ${typeof v}`);
  }

  validateMutate(tx, v);

  if ('clientID' in v) {
    return v as V & {clientID: string};
  }
  return {...v, clientID: tx.clientID};
}

export function normalizeScanOptions<T extends PresenceEntity>(
  options?: PresenceListOptions<T>,
): ListOptionsWith<ListID<T>> | undefined {
  if (!options) {
    return options;
  }
  const {startAtID, limit} = options;
  return {
    startAtID:
      startAtID &&
      (normalizePresenceID({clientID: ''}, startAtID) as ListID<T>),
    limit,
  };
}

function validateMutate(
  tx: {clientID: string},
  id: {clientID?: string | undefined},
): void {
  if (id.clientID && id.clientID !== tx.clientID) {
    throw new Error(
      `Can only mutate own entities. Expected clientID "${tx.clientID}" but received "${id.clientID}"`,
    );
  }
}

type Console = {
  debug(...args: unknown[]): void;
  info(...args: unknown[]): void;
  error(...args: unknown[]): void;
};

declare const console: Console;

export function generatePresence<T extends Required<PresenceEntity>>(
  name: string,
  parse?: Parse<T> | undefined,
  logger?: OptionalLogger,
): GeneratePresenceResult<T>;
export function generatePresence<T extends PresenceEntity>(
  name: string,
  parse?: Parse<T> | undefined,
  logger?: OptionalLogger,
): GeneratePresenceResult<T>;
export function generatePresence<T extends PresenceEntity>(
  name: string,
  parse: Parse<T> | undefined = undefined,
  logger: OptionalLogger = console,
): GeneratePresenceResult<T> {
  if (name === '' || name.includes('/')) {
    throw new Error(`Invalid name: ${name}. Must not be empty or include '/'`);
  }

  const keyFromEntityLocal: KeyFromEntityFunc<PresenceEntity> = (_tx, entity) =>
    keyFromID(name, entity);
  const keyFromIDLocal: KeyFromLookupIDFunc<PresenceEntity> = id =>
    keyFromID(name, id);
  const parseKeyToIDLocal: KeyToIDFunc<ListID<T>> = (key: string) =>
    parseKeyToID(name, key) as ListID<T>;
  const firstKey = () => presencePrefix;
  const parseInternal: ParseInternal<T> = (_, v) => maybeParse(parse, v);
  const parseAndValidateClientIDForMutate: ParseInternal<T> = (tx, v) =>
    parseInternal(tx, normalizeForSet(tx, v as unknown as OptionalClientID<T>));
  const set: GeneratePresenceResult<T>['set'] = (tx, value) =>
    setImpl(keyFromEntityLocal, parseAndValidateClientIDForMutate, tx, value);

  return {
    set,
    put: set,
    init: (tx, value) =>
      initImpl(
        keyFromEntityLocal,
        parseAndValidateClientIDForMutate,
        tx,
        value,
      ),
    update: (tx, update) =>
      updateImpl(
        keyFromEntityLocal,
        idFromEntity,
        parseInternal,
        parseAndValidateClientIDForMutate,
        tx,
        normalizeForUpdate(tx, update),
        logger,
      ),
    delete: (tx, id?) =>
      deleteImpl(
        keyFromIDLocal,
        validateMutate,
        tx,
        normalizePresenceID(tx, id),
      ),
    has: (tx, id?) => hasImpl(keyFromIDLocal, tx, normalizePresenceID(tx, id)),
    get: (tx, id?) =>
      getImpl(keyFromIDLocal, parseInternal, tx, normalizePresenceID(tx, id)),
    mustGet: (tx, id?) =>
      mustGetImpl(
        keyFromIDLocal,
        parseInternal,
        tx,
        normalizePresenceID(tx, id),
      ),
    list: (tx, options?) =>
      listImpl(
        keyFromIDLocal,
        parseKeyToIDLocal,
        firstKey,
        parse,
        tx,
        normalizeScanOptions(options),
      ),
    listIDs: (tx, options?) =>
      listIDsImpl<ListID<T>>(
        keyFromIDLocal,
        parseKeyToIDLocal,
        firstKey,
        tx,
        normalizeScanOptions(options),
      ),
    listClientIDs: (tx, options?) =>
      listClientIDsImpl(
        keyFromIDLocal,
        parseKeyToIDLocal,
        firstKey,
        tx,
        normalizeScanOptions(options),
      ),
    listEntries: (tx, options?) =>
      listEntriesImpl(
        keyFromIDLocal,
        parseKeyToIDLocal,
        firstKey,
        parse,
        tx,
        normalizeScanOptions(options),
      ),
  };
}

async function listClientIDsImpl<ID extends PresenceEntity>(
  keyFromID: KeyFromLookupIDFunc<ID>,
  keyToID: KeyToIDFunc<ID>,
  firstKey: FirstKeyFunc,
  tx: ReadTransaction,
  options?: ListOptionsWith<ID>,
): Promise<string[]> {
  // For this function we might get more than one entry per clientID in case there are entries like:
  //
  //   -/p/clientID1/name/id/id1
  //   -/p/clientID1/name/id/id2
  //   -/p/clientID1/name/id/id3
  //
  // We therefore remove the limit passed into scan and manage the limit ourselves.
  // We also need to make sure we don't return the same clientID twice.

  const result: string[] = [];
  const keyToID2 = (key: string): string | undefined => {
    const id = keyToID(key);
    return id?.clientID;
  };
  let last = undefined;
  const fixedOptions = {
    ...options,
    limit: undefined,
  };
  let {limit: i = Infinity} = options ?? {};
  for await (const [k] of scan(
    keyFromID,
    keyToID2,
    firstKey,
    tx,
    fixedOptions,
  )) {
    if (k !== last) {
      if (--i < 0) {
        break;
      }
      last = k;
      result.push(k);
    }
  }
  return result;
}
