- must refactor to remove `then` from `query`. `then` causes type inference to unwrap all the way down the chain.

E.g., if `Promise<thennable<T>>` we get `T`

https://discord.com/channels/830183651022471199/1412379022876479559/1416098721577435387


