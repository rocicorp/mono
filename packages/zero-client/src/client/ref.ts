type Listener<T> = (v: T | undefined) => void;

/**
 * A reference to a value that can be observed when
 * it changes.
 *
 * Listeners that are added will be called immediately
 * with the current value.
 */
export class Ref<T> {
  #listeners = new Set<Listener<T>>();
  #current: T | undefined;

  set value(value: T | undefined) {
    this.#current = value;
    for (const listener of this.#listeners) {
      listener(value);
    }
  }

  get value() {
    return this.#current;
  }

  getSnapshot = () => this.#current;

  onChange = (listener: Listener<T>) => {
    this.#listeners.add(listener);
    listener(this.#current);
    return () => {
      this.#listeners.delete(listener);
    };
  };
}
