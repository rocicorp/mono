/**
 * The ID describing a client.
 */
type ClientID = string;

type SubscribeToPresenceCallback = (presentClientIDs: ReadonlySet<ClientID>) => void;
type PresenceSubscribable = {
    subscribeToPresence(callback: SubscribeToPresenceCallback): () => void;
};
declare function usePresence(r: PresenceSubscribable | null | undefined): ReadonlySet<ClientID>;

declare type Subscribable<Tx, Data> = {
    subscribe: (query: (tx: Tx) => Promise<Data>, { onData }: {
        onData: (data: Data) => void;
    }) => () => void;
};
declare function useSubscribe<Tx, D, R extends D>(r: Subscribable<Tx, D> | null | undefined, query: (tx: Tx) => Promise<R>, def: R, deps?: Array<unknown>): R;

export { usePresence, useSubscribe };
