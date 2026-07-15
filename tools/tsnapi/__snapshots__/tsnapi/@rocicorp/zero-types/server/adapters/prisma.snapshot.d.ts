// #region Types
export type PrismaClientLike<TTransaction extends PrismaTransactionLike = PrismaTransactionLike> = {
    $transaction: <T>(_: (_: TTransaction) => Promise<T>) => Promise<T>;
};
export type PrismaTransaction<TClient extends PrismaClientLike = PrismaClientLike> = TClient extends PrismaClientLike<infer TTransaction> ? TTransaction : PrismaTransactionLike;
export type PrismaTransactionLike = {
    $queryRawUnsafe: (_: string, ..._: unknown[]) => Promise<unknown>;
};
// #endregion

// #region Classes
export declare class PrismaConnection<TClient extends PrismaClientLike> implements DBConnection<PrismaTransaction<TClient>> {
    #private;
    constructor(_: TClient);
    transaction<T>(_: (_: DBTransaction<PrismaTransaction<TClient>>) => Promise<T>): Promise<T>;
}
// #endregion

// #region Functions
export declare function zeroPrisma<TSchema extends Schema, TClient extends PrismaClientLike>(_: TSchema, _: TClient): ZQLDatabase<TSchema, PrismaTransaction<TClient>>;
// #endregion

// #region Other
export { ZQLDatabase }
// #endregion
