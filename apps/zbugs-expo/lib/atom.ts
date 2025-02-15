export class Atom<T> {
  #subs = new Set<(value: T | undefined) => void>();
  #val: T | undefined;

  set value(value: T | undefined) {
    this.#val = value;
    this.#subs.forEach(listener => listener(value));
  }

  get value() {
    return this.#val;
  }

  onChange = (cb: (value: T | undefined) => void) => {
    this.#subs.add(cb);
    cb(this.#val);
    return () => this.#subs.delete(cb);
  };
}
