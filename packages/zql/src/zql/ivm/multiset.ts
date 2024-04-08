export type Entry<T> = readonly [T, Multiplicity];
export type Multiplicity = number;
export type Multiset<T> = Iterable<Entry<T>>;
