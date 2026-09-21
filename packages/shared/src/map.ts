/**
 * A value that {@link Map.get} can distinguish from a missing entry.  `undefined`
 * is excluded because these helpers use `get() !== undefined` to detect absence.
 */
type Defined = {} | null;

const nativeSupport =
  typeof (Map.prototype as unknown as MapES2026<unknown, Defined>)
    .getOrInsert === 'function';

/**
 * A {@link Map} or a {@link WeakMap}; the ES2026 proposal adds these methods to
 * both.
 */
type MapOrWeakMap<K, V> = Map<K, V> | WeakMap<K & WeakKey, V>;

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
  defaultValue: V,
): V {
  const existing = (map as Map<K, V>).get(key);
  if (existing !== undefined) {
    return existing;
  }
  (map as Map<K, V>).set(key, defaultValue);
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
  compute: (key: K) => V,
): V {
  const existing = (map as Map<K, V>).get(key);
  if (existing !== undefined) {
    return existing;
  }
  const value = compute(key);
  (map as Map<K, V>).set(key, value);
  return value;
}

function getOrInsertNative<K, V extends Defined>(
  map: MapOrWeakMap<K, V>,
  key: K,
  defaultValue: V,
): V {
  return (map as unknown as MapES2026<K, V>).getOrInsert(key, defaultValue);
}

function getOrInsertComputedNative<K, V extends Defined>(
  map: MapOrWeakMap<K, V>,
  key: K,
  compute: (key: K) => V,
): V {
  return (map as unknown as MapES2026<K, V>).getOrInsertComputed(key, compute);
}

export const getOrInsert = nativeSupport
  ? getOrInsertNative
  : getOrInsertPolyfill;

export const getOrInsertComputed = nativeSupport
  ? getOrInsertComputedNative
  : getOrInsertComputedPolyfill;
