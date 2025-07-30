export class Subscribable<
  TArgs,
  TListener extends (obj: TArgs) => unknown = (obj: TArgs) => unknown,
> {
  protected _listeners = new Set<TListener>();

  /**
   * Subscribe to the subscribable.
   *
   * @param listener - The listener to subscribe to.
   * @returns A function to unsubscribe from the subscribable.
   */
  subscribe = (listener: TListener): (() => void) => {
    this._listeners.add(listener);

    return () => {
      this._listeners.delete(listener);
    };
  };

  /**
   * Notify all listeners.
   *
   * @param update - The update to notify listeners with.
   */
  notify = (update: TArgs): void => {
    this._listeners.forEach(listener => listener(update));
  };

  hasListeners = (): boolean => this._listeners.size > 0;

  /**
   * Unsubscribe all listeners.
   */
  cleanup = (): void => {
    this._listeners.clear();
  };
}
