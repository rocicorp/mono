/**
 * A value that {@link Map.get} can distinguish from a missing entry.  `undefined`
 * is excluded because these helpers use `get() !== undefined` to detect absence.
 */
type Defined = {} | null;

/**
 * A {@link Map} or a {@link WeakMap}.  The ES2026 proposal adds `getOrInsert`
 * and `getOrInsertComputed` to both.
 */
type MapOrWeakMap<K, V> = Map<K, V> | WeakMap<K & WeakKey, V>;

const nativeSupport =
  typeof (Map.prototype as unknown as MapES2026<unknown, Defined>)
    .getOrInsert === 'function' &&
  typeof (WeakMap.prototype as unknown as MapES2026<WeakKey, Defined>)
    .getOrInsert === 'function';

interface MapES2026<K, V> {
  getOrInsert(key: K, defaultValue: V): V;
  getOrInsertComputed(key: K, compute: (key: K) => V): V;
}

/**
 * Returns the value for {@link key} in {@link map}.  If no mapping exists,
 * inserts {@link defaultValue} and returns it.
 *
 * Mirrors the ES2026 `Map.prototype.getOrInsert` proposal.
 */
function getOrInsertPolyfill<K, V extends Defined>(
  map: MapOrWeakMap<K, V>,
  key: K,
  defaultValue: NoInfer<V>,
): V {
  const existing = map.get(key as K & WeakKey);
  if (existing !== undefined) {
    return existing;
  }
  map.set(key as K & WeakKey, defaultValue);
  return defaultValue;
}

/**
 * Returns the value for {@link key} in {@link map}.  If no mapping exists,
 * calls {@link compute} with the key, inserts the result, and returns it.
 *
 * Mirrors the ES2026 `Map.prototype.getOrInsertComputed` proposal.
 */
function getOrInsertComputedPolyfill<K, V extends Defined>(
  map: MapOrWeakMap<K, V>,
  key: K,
  compute: (key: K) => NoInfer<V>,
): V {
  const existing = map.get(key as K & WeakKey);
  if (existing !== undefined) {
    return existing;
  }
  const value = compute(key);
  map.set(key as K & WeakKey, value);
  return value;
}

function getOrInsertNative<K, V extends Defined>(
  map: MapOrWeakMap<K, V>,
  key: K,
  defaultValue: NoInfer<V>,
): V {
  return (map as unknown as MapES2026<K, V>).getOrInsert(key, defaultValue);
}

function getOrInsertComputedNative<K, V extends Defined>(
  map: MapOrWeakMap<K, V>,
  key: K,
  compute: (key: K) => NoInfer<V>,
): V {
  return (map as unknown as MapES2026<K, V>).getOrInsertComputed(key, compute);
}

export const getOrInsert = nativeSupport
  ? getOrInsertNative
  : getOrInsertPolyfill;

export const getOrInsertComputed = nativeSupport
  ? getOrInsertComputedNative
  : getOrInsertComputedPolyfill;

/**
 * Creates an empty {@link Map}.  Pass this to {@link getOrInsertComputed}
 * instead of an inline `() => new Map()` so that a hit does not allocate a
 * closure.
 */
export const newMap = <K, V>(): Map<K, V> => new Map();

/**
 * Creates an empty {@link WeakMap}.  Pass this to {@link getOrInsertComputed}
 * instead of an inline `() => new WeakMap()` so that a hit does not allocate a
 * closure.
 */
export const newWeakMap = <K extends WeakKey, V>(): WeakMap<K, V> =>
  new WeakMap();
