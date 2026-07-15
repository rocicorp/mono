// #region Types
export type DrizzleDatabase<TTransaction extends DrizzleTransactionLike = DrizzleTransactionLike> = {
    transaction<T>(_: (_: TTransaction) => Promise<T>, _?: never): Promise<T>;
};
export type DrizzleTransaction<TDbOrSchema = Record<string, unknown>> = TDbOrSchema extends DrizzleDatabase<infer TTransaction> ? TTransaction : DrizzleTransactionFromSchema<TDbOrSchema>;
// #endregion

// #region Classes
export declare class DrizzleConnection<TDrizzle, TTransaction extends DrizzleTransactionLike = DrizzleTransaction<TDrizzle>> implements DBConnection<TTransaction> {
    #private;
    constructor(_: TDrizzle & DrizzleDatabase<TTransaction>);
    transaction<T>(_: (_: DBTransaction<TTransaction>) => Promise<T>): Promise<T>;
}
// #endregion

// #region Functions
export declare function toIterableRows(_: unknown): Iterable<Row>;
export declare function zeroDrizzle<TSchema extends Schema, TDrizzle, TTransaction extends DrizzleTransactionLike = DrizzleTransaction<TDrizzle>>(_: TSchema, _: TDrizzle & DrizzleDatabase<TTransaction>): ZQLDatabase<TSchema, TTransaction>;
// #endregion

// #region Other
export { ZQLDatabase }
// #endregion
