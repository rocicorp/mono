// #region Types
export type KyselyDatabase<TDatabase = unknown> = Kysely<TDatabase>;
export type KyselyTransaction<TDbOrSchema = KyselyDatabase> = TDbOrSchema extends Kysely<infer TInferredDatabase> ? WrappedKyselyTransaction<TInferredDatabase> : WrappedKyselyTransaction<TDbOrSchema>;
// #endregion

// #region Classes
export declare class KyselyConnection<TDatabase> implements DBConnection<WrappedKyselyTransaction<TDatabase>> {
    #private;
    constructor(_: Kysely<TDatabase>);
    transaction<T>(_: (_: DBTransaction<WrappedKyselyTransaction<TDatabase>>) => Promise<T>): Promise<T>;
}
// #endregion

// #region Functions
export declare function zeroKysely<TSchema extends Schema, TDatabase>(_: TSchema, _: Kysely<TDatabase>): ZQLDatabase<TSchema, WrappedKyselyTransaction<TDatabase>>;
// #endregion

// #region Other
export { ZQLDatabase }
// #endregion
