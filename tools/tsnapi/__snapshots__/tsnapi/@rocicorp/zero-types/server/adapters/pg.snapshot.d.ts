// #region Types
export type NodePgTransaction = Pool | PoolClient | Client;
// #endregion

// #region Classes
export declare class NodePgConnection implements DBConnection<NodePgTransaction> {
    #private;
    constructor(_: NodePgTransaction);
    transaction<TRet>(_: (_: DBTransaction<NodePgTransaction>) => Promise<TRet>): Promise<TRet>;
}
export declare class NodePgTransactionInternal implements DBTransaction<NodePgTransaction> {
    readonly wrappedTransaction: NodePgTransaction;
    constructor(_: NodePgTransaction);
    runQuery<TReturn>(_: AST, _: Format, _: Schema, _: ServerSchema): Promise<HumanReadable<TReturn>>;
    query(_: string, _: unknown[]): Promise<Row[]>;
}
// #endregion

// #region Functions
export declare function zeroNodePg<S extends Schema>(_: S, _: NodePgTransaction | string): ZQLDatabase<S, NodePgTransaction>;
// #endregion

// #region Other
export { ZQLDatabase }
// #endregion
