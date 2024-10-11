type Listener<T> = (v: T | undefined) => void;

export class Ref<T> {
  #listeners = new Set<Listener<T>>();
  #current: T | undefined;

  set(value: T | undefined) {
    this.#current = value;
    for (const listener of this.#listeners) {
      listener(value);
    }
  }

  get() {
    return this.#current;
  }

  onChange(listener: Listener<T>) {
    this.#listeners.add(listener);
    listener(this.#current);
    return () => {
      this.#listeners.delete(listener);
    };
  }
}
