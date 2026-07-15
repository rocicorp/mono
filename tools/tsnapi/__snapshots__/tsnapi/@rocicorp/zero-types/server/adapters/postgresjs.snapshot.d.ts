// #region Types
export type PostgresJsTransaction<T extends Record<string, unknown> = Record<string, unknown>> = postgres.TransactionSql<T>;
// #endregion

// #region Classes
export declare class PostgresJSConnection<T extends Record<string, unknown>> implements DBConnection<PostgresJsTransaction<T>> {
    #private;
    constructor(_: postgres.Sql<T>);
    transaction<TRet>(_: (_: DBTransaction<PostgresJsTransaction<T>>) => Promise<TRet>): Promise<TRet>;
}
export declare class PostgresJsTransactionInternal<T extends Record<string, unknown>> implements DBTransaction<PostgresJsTransaction<T>> {
    readonly wrappedTransaction: PostgresJsTransaction<T>;
    constructor(_: PostgresJsTransaction<T>);
    query(_: string, _: unknown[]): Promise<Row[]>;
    runQuery<TReturn>(_: AST, _: Format, _: Schema, _: ServerSchema): Promise<HumanReadable<TReturn>>;
}
// #endregion

// #region Functions
export declare function zeroPostgresJS<S extends Schema, T extends Record<string, unknown> = Record<string, unknown>>(_: S, _: postgres.Sql<T> | string): ZQLDatabase<S, PostgresJsTransaction<T>>;
// #endregion

// #region Other
export { ZQLDatabase }
// #endregion
