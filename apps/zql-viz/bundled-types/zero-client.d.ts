import type { LogLevel } from '@rocicorp/logger';
import type { LogSink } from '@rocicorp/logger';
import * as v_2 from '@badrap/valita';

declare const AbstractType: any;

/**
 * Represents a node (and all its children) getting added to the result.
 */
declare type AddChange = {
    type: 'add';
    node: Node_2;
};

declare type AddSubreturn<TExistingReturn, TSubselectReturn, TAs extends string> = {
    readonly [K in TAs]: undefined extends TSubselectReturn ? TSubselectReturn : readonly TSubselectReturn[];
} extends infer TNewRelationship ? undefined extends TExistingReturn ? (Exclude<TExistingReturn, undefined> & TNewRelationship) | undefined : TExistingReturn & TNewRelationship : never;

declare type AddViewChange = {
    type: 'add';
    node: Node_2;
};

declare function and(...conditions: (Condition | undefined)[]): Condition;

export declare const ANYONE_CAN: ((_: unknown, eb: ExpressionBuilder<Schema, never>) => Condition)[];

export declare const ANYONE_CAN_DO_ANYTHING: {
    row: {
        select: ((_: unknown, eb: ExpressionBuilder<Schema, never>) => Condition)[];
        insert: ((_: unknown, eb: ExpressionBuilder<Schema, never>) => Condition)[];
        update: {
            preMutation: ((_: unknown, eb: ExpressionBuilder<Schema, never>) => Condition)[];
            postMutation: ((_: unknown, eb: ExpressionBuilder<Schema, never>) => Condition)[];
        };
        delete: ((_: unknown, eb: ExpressionBuilder<Schema, never>) => Condition)[];
    };
};

export declare type AnyQuery = Query<Schema, string, any>;

export declare function applyChange(parentEntry: Entry, change: ViewChange, schema: SourceSchema, relationship: string, format: Format, withIDs?: boolean): void;

declare type ArraySelectors<E extends TableSchema> = {
    [K in keyof E['columns']]: E['columns'][K] extends SchemaValueWithCustomType<any[]> ? K : never;
}[keyof E['columns']];

declare function assert<T>(value: unknown, schema: v_2.Type<T>, mode?: ParseOptionsMode): asserts value is T;

export declare type AssetPermissions<TAuthDataShape, TSchema extends Schema, TTable extends keyof TSchema['tables'] & string> = {
    select?: PermissionRule<TAuthDataShape, TSchema, TTable>[] | undefined;
    insert?: PermissionRule<TAuthDataShape, TSchema, TTable>[] | undefined;
    update?: {
        preMutation?: PermissionRule<TAuthDataShape, TSchema, TTable>[];
        postMutation?: PermissionRule<TAuthDataShape, TSchema, TTable>[];
    } | undefined;
    delete?: PermissionRule<TAuthDataShape, TSchema, TTable>[] | undefined;
};

declare const assetSchema: v.ObjectType<{
    select: v.Optional<["allow", Condition][]>;
    insert: v.Optional<["allow", Condition][]>;
    update: v.Optional<{
        preMutation?: ["allow", Condition][] | undefined;
        postMutation?: ["allow", Condition][] | undefined;
    }>;
    delete: v.Optional<["allow", Condition][]>;
}, undefined>;

export declare type AST = {
    readonly schema?: string | undefined;
    readonly table: string;
    readonly alias?: string | undefined;
    readonly where?: Condition | undefined;
    readonly related?: readonly CorrelatedSubquery[] | undefined;
    readonly start?: Bound | undefined;
    readonly limit?: number | undefined;
    readonly orderBy?: Ordering | undefined;
};

declare const AuthInvalidated = "AuthInvalidated";

declare type AuthInvalidated = typeof AuthInvalidated;

declare type AvailableRelationships<TTable extends string, TSchema extends Schema> = keyof TSchema['relationships'][TTable] & string;

export declare type BatchMutator<S extends Schema> = <R>(body: (m: DBMutator<S>) => MaybePromise<R>) => Promise<R>;

export declare function boolean<T extends boolean = boolean>(): ColumnBuilder<{
    type: "boolean";
    optional: false;
    customType: T;
}>;

export declare type Bound = {
    row: Row_2;
    exclusive: boolean;
};

/**
 * Interface required of caller to buildPipeline. Connects to constructed
 * pipeline to delegate environment to provide sources and storage.
 */
declare interface BuilderDelegate {
    /**
     * Called once for each source needed by the AST.
     * Might be called multiple times with same tableName. It is OK to return
     * same storage instance in that case.
     */
    getSource(tableName: string): Source | undefined;
    /**
     * Called once for each operator that requires storage. Should return a new
     * unique storage object for each call.
     */
    createStorage(name: string): Storage_2;
    decorateInput(input: Input, name: string): Input;
    decorateFilterInput(input: FilterInput, name: string): FilterInput;
    /**
     * The AST is mapped on-the-wire between client and server names.
     *
     * There is no "wire" for zqlite tests so this function is provided
     * to allow tests to remap the AST.
     */
    mapAst?: ((ast: AST) => AST) | undefined;
}

declare type Cardinality = 'one' | 'many';

export declare type Change = AddChange | RemoveChange | ChildChange | EditChange;

/**
 * The node's row is unchanged, but one of its descendants has changed.
 * The node's relationships will reflect the change, `child` specifies the
 * specific descendant change.
 */
declare type ChildChange = {
    type: 'child';
    node: Node_2;
    child: {
        relationshipName: string;
        change: Change;
    };
};

declare type ChildViewChange = {
    type: 'child';
    node: RowOnlyNode;
    child: {
        relationshipName: string;
        change: ViewChange;
    };
};

/**
 * The ID describing a group of clients. All clients in the same group share a
 * persistent storage (IDB).
 */
export declare type ClientGroupID = string;

/**
 * The ID describing a client.
 */
export declare type ClientID = string;

declare type ClientID_2 = string;

declare const ClientNotFound = "ClientNotFound";

declare type ClientNotFound = typeof ClientNotFound;

/**
 * In certain scenarios the server can signal that it does not know about the
 * client. For example, the server might have lost all of its state (this might
 * happen during the development of the server).
 */
declare type ClientStateNotFoundResponse = {
    error: 'ClientStateNotFound';
};

/**
 * An instance of this is passed to custom mutator implementations and
 * allows reading and writing to the database and IVM at the head
 * at which the mutator is being applied.
 */
declare interface ClientTransaction<S extends Schema> extends TransactionBase<S> {
    readonly location: 'client';
    readonly reason: 'optimistic' | 'rebase';
}

export declare class ColumnBuilder<TShape extends SchemaValue<any>> {
    #private;
    constructor(schema: TShape);
    from<ServerName extends string>(serverName: ServerName): ColumnBuilder<TShape & {
        serverName: string;
    }>;
    optional(): ColumnBuilder<Omit<TShape, 'optional'> & {
        optional: true;
    }>;
    get schema(): TShape;
}

export declare type ColumnReference = {
    readonly type: 'column';
    /**
     * Not a path yet as we're currently not allowing
     * comparisons across tables. This will need to
     * be a path through the tree in the near future.
     */
    readonly name: string;
};

declare type ColumnTypeName<T extends SchemaValue | ValueType> = T extends SchemaValue ? T['type'] : T;

declare type CommitListener = () => void;

declare type Comparator = (r1: Row_2, r2: Row_2) => number;

export declare type CompiledAssetPermissions = v.Infer<typeof assetSchema>;

export declare type CompiledPermissionsConfig = v.Infer<typeof permissionsConfigSchema>;

export declare type CompiledPermissionsPolicy = v.Infer<typeof policySchema>;

export declare type CompiledPermissionsRule = v.Infer<typeof ruleSchema>;

export declare type CompoundKey = readonly [string, ...string[]];

/**
 * Starting only with SimpleCondition for now.
 * ivm1 supports Conjunctions and Disjunctions.
 * We'll support them in the future.
 */
export declare type Condition = SimpleCondition | Conjunction | Disjunction | CorrelatedSubqueryCondition;

export declare type Conjunction = {
    type: 'and';
    conditions: readonly Condition[];
};

declare type ConnectArg<TSourceField, TDestField, TDest extends TableSchema> = {
    readonly sourceField: TSourceField;
    readonly destField: TDestField;
    readonly destSchema: TableBuilderWithColumns<TDest>;
};

declare type Connection = {
    readonly sourceField: readonly string[];
    readonly destField: readonly string[];
    readonly destSchema: string;
    readonly cardinality: Cardinality;
};

declare type Constraint = {
    readonly [key: string]: Value;
};

declare type ContextualizedNamedQuery<TContext, TArg extends ReadonlyArray<ReadonlyJSONValue> = ReadonlyArray<ReadonlyJSONValue>, TReturnQuery extends Query<any, any, any> = Query<any, any, any>> = {
    (context: TContext, ...args: TArg): TReturnQuery;
    contextualized?: boolean;
};

/**
 * A cookie is a value that is used to determine the order of snapshots. It
 * needs to be comparable. This can be a `string`, `number` or if you want to
 * use a more complex value, you can use an object with an `order` property. The
 * value `null` is considered to be less than any other cookie and it is used
 * for the first pull when no cookie has been set.
 *
 * The order is the natural order of numbers and strings. If one of the cookies
 * is an object then the value of the `order` property is treated as the cookie
 * when doing comparison.
 *
 * If one of the cookies is a string and the other is a number, the number is
 * fist converted to a string (using `toString()`).
 */
declare type Cookie = null | string | number | (ReadonlyJSONValue & {
    readonly order: number | string;
});

export declare type CorrelatedSubquery = {
    /**
     * Only equality correlation are supported for now.
     * E.g., direct foreign key relationships.
     */
    readonly correlation: Correlation;
    readonly subquery: AST;
    readonly system?: System | undefined;
    readonly hidden?: boolean | undefined;
};

export declare type CorrelatedSubqueryCondition = {
    type: 'correlatedSubquery';
    related: CorrelatedSubquery;
    op: CorrelatedSubqueryConditionOperator;
};

export declare type CorrelatedSubqueryConditionOperator = 'EXISTS' | 'NOT EXISTS';

declare type Correlation = {
    readonly parentField: CompoundKey;
    readonly childField: CompoundKey;
};

/**
 * Returns a set of query builders for the given schema.
 */
export declare function createBuilder<S extends Schema>(s: S): SchemaQuery<S>;

/**
 * Factory function for creating {@link Store} instances.
 *
 * The name is used to identify the store. If the same name is used for multiple
 * stores, they should share the same data. It is also desirable to have these
 * stores share an `RWLock`.
 *
 */
export declare type CreateKVStore = (name: string) => KVStore;

/**
 * Note: the keys of the `tables` and `relationships` parameters do not matter.
 * You can assign them to any value you like. E.g.,
 *
 * ```ts
 * createSchema({rsdfgafg: table('users')...}, {sdfd: relationships(users, ...)})
 * ```
 */
export declare function createSchema<const TTables extends readonly TableBuilderWithColumns<TableSchema>[], const TRelationships extends readonly Relationships[]>(options: {
    readonly tables: TTables;
    readonly relationships?: TRelationships | undefined;
}): {
    tables: {
        readonly [K in TTables[number]['schema']['name']]: Extract<TTables[number]['schema'], {
            name: K;
        }>;
    };
    relationships: {
        readonly [K in TRelationships[number]['name']]: Extract<TRelationships[number], {
            name: K;
        }>['relationships'];
    };
};

/**
 * The shape which a user's custom mutator definitions must conform to.
 */
export declare type CustomMutatorDefs = {
    [namespaceOrKey: string]: {
        [key: string]: CustomMutatorImpl<any>;
    } | CustomMutatorImpl<any>;
};

export declare type CustomMutatorImpl<S extends Schema, TWrappedTransaction = unknown, TArgs = any> = (tx: Transaction<S, TWrappedTransaction>, args: TArgs) => Promise<void>;

export declare type CustomQueryID = {
    name: string;
    args: ReadonlyArray<ReadonlyJSONValue>;
};

export declare type DBMutator<S extends Schema> = {
    [K in keyof S['tables']]: TableMutator<S['tables'][K]>;
};

declare interface DBTransaction<T> extends Queryable {
    readonly wrappedTransaction: T;
}

declare type DeepMerge<A, B> = {
    [K in keyof A | keyof B]: K extends keyof B ? K extends keyof A ? IsPlainObject<A[K]> extends true ? IsPlainObject<B[K]> extends true ? DeepMerge<A[K], B[K]> : B[K] : B[K] : B[K] : K extends keyof A ? A[K] : never;
};

/**
 * Similar to `ObjectType.partial()` except it recurses into nested objects.
 * Rest types are not supported.
 */
declare function deepPartial<Shape extends ObjectShape>(s: v_2.ObjectType<Shape, undefined>): v_2.ObjectType<{ [K in keyof Shape]: v_2.Optional<v_2.Infer<Shape[K]>>; }, undefined>;

export declare function definePermissions<TAuthDataShape, TSchema extends Schema>(schema: TSchema, definer: () => Promise<PermissionsConfig<TAuthDataShape, TSchema>> | PermissionsConfig<TAuthDataShape, TSchema>): Promise<CompiledPermissionsConfig | undefined>;

export declare type DeleteID<S extends TableSchema> = Expand<PrimaryKeyFields<S>>;

declare type DestRow<TTable extends string, TSchema extends Schema, TRelationship extends string> = TSchema['relationships'][TTable][TRelationship][0]['cardinality'] extends 'many' ? PullRow<DestTableName<TTable, TSchema, TRelationship>, TSchema> : PullRow<DestTableName<TTable, TSchema, TRelationship>, TSchema> | undefined;

declare type DestTableName<TTable extends string, TSchema extends Schema, TRelationship extends string> = LastInTuple<TSchema['relationships'][TTable][TRelationship]>['destSchema'];

export declare type Disjunction = {
    type: 'or';
    conditions: readonly Condition[];
};

/**
 * Deletes all IndexedDB data associated with Replicache.
 *
 * Returns an object with the names of the successfully dropped databases
 * and any errors encountered while dropping.
 */
export declare function dropAllDatabases(opts?: DropDatabaseOptions | undefined): Promise<{
    dropped: string[];
    errors: unknown[];
}>;

/**
 * Drops the specified database.
 * @param dbName The name of the database to drop.
 * @param opts Options for dropping the database.
 */
export declare function dropDatabase(dbName: string, opts?: DropDatabaseOptions | undefined): Promise<void>;

/**
 * Options for `dropDatabase` and `dropAllDatabases`.
 */
declare type DropDatabaseOptions = {
    /**
     * Allows providing a custom implementation of the underlying storage layer.
     * Default is `'idb'`.
     */
    kvStore?: 'idb' | 'mem' | StoreProvider | undefined;
    /**
     * Determines how much logging to do. When this is set to `'debug'`,
     * Replicache will also log `'info'` and `'error'` messages. When set to
     * `'info'` we log `'info'` and `'error'` but not `'debug'`. When set to
     * `'error'` we only log `'error'` messages.
     * Default is `'info'`.
     */
    logLevel?: LogLevel | undefined;
    /**
     * Enables custom handling of logs.
     *
     * By default logs are logged to the console.  If you would like logs to be
     * sent elsewhere (e.g. to a cloud logging service like DataDog) you can
     * provide an array of {@link LogSink}s.  Logs at or above
     * {@link DropDatabaseOptions.logLevel} are sent to each of these {@link LogSink}s.
     * If you would still like logs to go to the console, include
     * `consoleLogSink` in the array.
     *
     * ```ts
     * logSinks: [consoleLogSink, myCloudLogSink],
     * ```
     * Default is `[consoleLogSink]`.
     */
    logSinks?: LogSink[] | undefined;
};

/**
 * Function for deleting {@link Store} instances.
 *
 * The name is used to identify the store. If the same name is used for multiple
 * stores, they should share the same data.
 *
 */
declare type DropStore = (name: string) => Promise<void>;

/**
 * The row changed (in a way that the {@linkcode Source} determines). Most
 * likely the PK stayed the same but there is really no restriction in how it
 * can change.
 *
 * The edit changes flows down in a {@linkcode Output.push}.
 * There are cases where an edit change gets split into a remove and/or an add
 * change.
 * 1. when the presence of the row in the result changes (for example the row
 *    is no longer present due to a filter)
 * 2. the edit results in the rows relationships changing
 *
 * If an edit is not split, the relationships of node and oldNode must
 * be the same, just the Row has changed.
 *
 * NOTE: It would be cleaner to just have the relationships once,
 * since they must be the same, however relationship Streams are single use
 * and if an Edit needs to be split into a remove and add a single map
 * of relationship Streams could not be used for the both the remove and
 * the add.  This cleanup could be done if we move to multi-use Streams
 * for relationships.
 */
declare type EditChange = {
    type: 'edit';
    node: Node_2;
    oldNode: Node_2;
};

declare type EditViewChange = {
    type: 'edit';
    node: RowOnlyNode;
    oldNode: RowOnlyNode;
};

export declare type Entry = {
    readonly [key: string]: Value | View;
};

declare type EntryList = readonly Entry[];

export declare function enumeration<T extends string>(): ColumnBuilder<{
    type: "string";
    optional: false;
    customType: T;
}>;

export declare type EnumSchemaValue<T> = {
    kind: 'enum';
    type: 'string';
    serverName?: string | undefined;
    optional?: boolean;
    customType: T;
};

export declare type EqualityOps = '=' | '!=' | 'IS' | 'IS NOT';

declare namespace ErrorKind {
    export {
        AuthInvalidated,
        ClientNotFound,
        InvalidConnectionRequest,
        InvalidConnectionRequestBaseCookie,
        InvalidConnectionRequestLastMutationID,
        InvalidConnectionRequestClientDeleted,
        InvalidMessage,
        InvalidPush,
        MutationFailed,
        MutationRateLimited,
        Rebalance,
        Rehome,
        Unauthorized,
        VersionNotSupported,
        SchemaVersionNotSupported,
        ServerOverloaded,
        Internal
    }
}
export { ErrorKind }

export declare function escapeLike(val: string): string;

/**
 * Expand/simplifies a type for display in Intellisense.
 */
declare type Expand<T> = T extends infer O ? {
    [K in keyof O]: O[K];
} : never;

export declare class ExpressionBuilder<TSchema extends Schema, TTable extends keyof TSchema['tables'] & string> {
    #private;
    constructor(exists: (relationship: string, cb?: (query: Query<TSchema, TTable>) => Query<TSchema, any>) => Condition);
    get eb(): this;
    cmp<TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>, TOperator extends SimpleOperator>(field: TSelector, op: TOperator, value: GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, TOperator> | ParameterReference): Condition;
    cmp<TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>>(field: TSelector, value: GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, '='> | ParameterReference): Condition;
    cmpLit(left: ParameterReference | LiteralValue, op: SimpleOperator, right: ParameterReference | LiteralValue): Condition;
    and: typeof and;
    or: typeof or;
    not: typeof not;
    exists: <TRelationship extends AvailableRelationships<TTable, TSchema>>(relationship: TRelationship, cb?: (query: Query<TSchema, DestTableName<TTable, TSchema, TRelationship>>) => Query<TSchema, any>) => Condition;
}

/**
 * A factory function that creates a condition. This is used to create
 * complex conditions that can be passed to the `where` method of a query.
 *
 * @example
 *
 * ```ts
 * const condition: ExpressionFactory<User> = ({and, cmp, or}) =>
 *   and(
 *     cmp('name', '=', 'Alice'),
 *     or(cmp('age', '>', 18), cmp('isStudent', '=', true)),
 *   );
 *
 * const query = z.query.user.where(condition);
 * ```
 */
export declare interface ExpressionFactory<TSchema extends Schema, TTable extends keyof TSchema['tables'] & string> {
    (eb: ExpressionBuilder<TSchema, TTable>): Condition;
}

declare type FetchRequest = {
    readonly constraint?: Constraint | undefined;
    /** If supplied, `start.row` must have previously been output by fetch or push. */
    readonly start?: Start | undefined;
    /** Whether to fetch in reverse order of the SourceSchema's sort. */
    readonly reverse?: boolean | undefined;
};

/**
 * The `where` clause of a ZQL query is implemented using a sub-graph of
 * `FilterOperators`.  This sub-graph starts with a `FilterStart` operator,
 * that adapts from the normal `Operator` `Output`, to the
 * `FilterOperator` `FilterInput`, and ends with a `FilterEnd` operator that
 * adapts from a `FilterOperator` `FilterOutput` to a normal `Operator` `Input`.
 * `FilterOperator'`s do not have `fetch` or `cleanup` instead they have a
 * `filter(node: Node, cleanup: boolean): boolean` method.
 * They also have `push` which is just like normal `Operator` push.
 * Not having a `fetch` means these `FilterOperator`'s cannot modify
 * `Node` `row`s or `relationship`s, but they shouldn't, they should just
 * filter.
 *
 * This `FilterOperator` abstraction enables much more efficient processing of
 * `fetch` for `where` clauses containing OR conditions.
 *
 * See https://github.com/rocicorp/mono/pull/4339
 */
declare interface FilterInput extends InputBase {
    /** Tell the input where to send its output. */
    setFilterOutput(output: FilterOutput): void;
}

declare interface FilterOutput extends Output {
    filter(node: Node_2, cleanup: boolean): boolean;
}

export declare type Format = {
    singular: boolean;
    relationships: Record<string, Format>;
};

/**
 * This creates a default puller which uses HTTP POST to send the pull request.
 */
export declare function getDefaultPuller(rep: {
    pullURL: string;
    auth: string;
}): Puller;

declare type GetFilterType<TSchema extends TableSchema, TColumn extends keyof TSchema['columns'], TOperator extends SimpleOperator> = TOperator extends 'IS' | 'IS NOT' ? // SchemaValueToTSType adds null if the type is optional, but we add null
SchemaValueToTSType<TSchema['columns'][TColumn]> | null : TOperator extends 'IN' | 'NOT IN' ? readonly Exclude<SchemaValueToTSType<TSchema['columns'][TColumn]>, null>[] : Exclude<SchemaValueToTSType<TSchema['columns'][TColumn]>, null>;

declare type GotCallback = (got: boolean) => void;

export declare type HTTPRequestInfo = {
    httpStatusCode: number;
    errorMessage: string;
};

declare type HTTPString = `http${'' | 's'}://${string}`;

/**
 * A helper type that tries to make the type more readable.
 */
export declare type HumanReadable<T> = undefined extends T ? Expand<T> : Expand<T>[];

/**
 * This error is thrown when we detect that the IndexedDB has been removed. This
 * does not normally happen but can happen during development if the user has
 * DevTools open and deletes the IndexedDB from there.
 */
export declare class IDBNotFoundError extends Error {
    name: string;
}

/**
 * Create a deeply immutable type from a type that may contain mutable types.
 */
declare type Immutable<T> = T extends Primitive ? T : T extends ReadonlyArray<infer U> ? ImmutableArray<U> : ImmutableObject<T>;

declare type ImmutableArray<T> = ReadonlyArray<Immutable<T>>;

declare type ImmutableObject<T> = {
    readonly [K in keyof T]: Immutable<T[K]>;
};

export declare type InOps = 'IN' | 'NOT IN';

export declare interface Input extends InputBase {
    /** Tell the input where to send its output. */
    setOutput(output: Output): void;
    /**
     * Fetch data. May modify the data in place.
     * Returns nodes sorted in order of `SourceSchema.compareRows`.
     */
    fetch(req: FetchRequest): Stream<Node_2>;
    /**
     * Cleanup maintained state. This is called when `output` will no longer need
     * the data returned by {@linkcode fetch}. The receiving operator should clean up any
     * resources it has allocated to service such requests.
     *
     * This is different from {@linkcode destroy} which means this input will no longer
     * be called at all, for any input.
     *
     * Returns the same thing as {@linkcode fetch}. This allows callers to properly
     * propagate the cleanup message through the graph.
     */
    cleanup(req: FetchRequest): Stream<Node_2>;
}

/**
 * Input to an operator.
 */
declare interface InputBase {
    /** The schema of the data this input returns. */
    getSchema(): SourceSchema;
    /**
     * Completely destroy the input. Destroying an input
     * causes it to call destroy on its upstreams, fully
     * cleaning up a pipeline.
     */
    destroy(): void;
}

export declare type InsertValue<S extends TableSchema> = Expand<PrimaryKeyFields<S> & {
    [K in keyof S['columns'] as S['columns'][K] extends {
        optional: true;
    } ? K : never]?: SchemaValueToTSType<S['columns'][K]> | undefined;
} & {
    [K in keyof S['columns'] as S['columns'][K] extends {
        optional: true;
    } ? never : K]: SchemaValueToTSType<S['columns'][K]>;
}>;

export declare interface Inspector {
    readonly client: InspectorClient;
    readonly clientGroup: InspectorClientGroup;
    clients(): Promise<InspectorClient[]>;
    clientsWithQueries(): Promise<InspectorClient[]>;
}

export declare interface InspectorClient {
    readonly id: string;
    readonly clientGroup: InspectorClientGroup;
    queries(): Promise<InspectorQuery[]>;
    map(): Promise<Map<string, ReadonlyJSONValue>>;
    rows(tableName: string): Promise<Row_2[]>;
}

export declare interface InspectorClientGroup {
    readonly id: string;
    clients(): Promise<InspectorClient[]>;
    clientsWithQueries(): Promise<InspectorClient[]>;
    queries(): Promise<InspectorQuery[]>;
}

export declare interface InspectorQuery {
    readonly ast: AST | null;
    readonly name: string | null;
    readonly args: ReadonlyArray<ReadonlyJSONValue> | null;
    readonly clientID: string;
    readonly deleted: boolean;
    readonly got: boolean;
    readonly id: string;
    readonly inactivatedAt: Date | null;
    readonly rowCount: number;
    readonly ttl: TTL;
    readonly zql: string | null;
}

declare function instanceOfAbstractType<T = unknown>(obj: unknown): obj is v_2.Type<T> | v_2.Optional<T>;

declare const Internal = "Internal";

declare type Internal = typeof Internal;

declare const InvalidConnectionRequest = "InvalidConnectionRequest";

declare type InvalidConnectionRequest = typeof InvalidConnectionRequest;

declare const InvalidConnectionRequestBaseCookie = "InvalidConnectionRequestBaseCookie";

declare type InvalidConnectionRequestBaseCookie = typeof InvalidConnectionRequestBaseCookie;

declare const InvalidConnectionRequestClientDeleted = "InvalidConnectionRequestClientDeleted";

declare type InvalidConnectionRequestClientDeleted = typeof InvalidConnectionRequestClientDeleted;

declare const InvalidConnectionRequestLastMutationID = "InvalidConnectionRequestLastMutationID";

declare type InvalidConnectionRequestLastMutationID = typeof InvalidConnectionRequestLastMutationID;

declare const InvalidMessage = "InvalidMessage";

declare type InvalidMessage = typeof InvalidMessage;

declare const InvalidPush = "InvalidPush";

declare type InvalidPush = typeof InvalidPush;

declare function is<T>(value: unknown, schema: v_2.Type<T>, mode?: ParseOptionsMode): value is T;

declare type IsPlainObject<T> = T extends object ? T extends Function | any[] ? false : true : false;

export declare function json<T extends ReadonlyJSONValue = ReadonlyJSONValue>(): ColumnBuilder<{
    type: "json";
    optional: false;
    customType: T;
}>;

/**
 * A JSON object. This is a map from strings to JSON values or `undefined`. We
 * allow `undefined` values as a convenience... but beware that the `undefined`
 * values do not round trip to the server. For example:
 *
 * ```
 * // Time t1
 * await tx.set('a', {a: undefined});
 *
 * // time passes, in a new transaction
 * const v = await tx.get('a');
 * console.log(v); // either {a: undefined} or {}
 * ```
 */
export declare type JSONObject = {
    [key: string]: JSONValue | undefined;
};

declare type JsonSelectors<E extends TableSchema> = {
    [K in keyof E['columns']]: E['columns'][K] extends {
        type: 'json';
    } ? K : never;
}[keyof E['columns']];

/** The values that can be represented in JSON */
export declare type JSONValue = null | string | boolean | number | Array<JSONValue> | JSONObject;

/**
 * @experimental This interface is experimental and might be removed or changed
 * in the future without following semver versioning. Please be cautious.
 */
export declare interface KVRead extends Release {
    has(key: string): Promise<boolean>;
    get(key: string): Promise<ReadonlyJSONValue | undefined>;
    closed: boolean;
}

/**
 * Store defines a transactional key/value store that Replicache stores all data
 * within.
 *
 * For correct operation of Replicache, implementations of this interface must
 * provide [strict
 * serializable](https://jepsen.io/consistency/models/strict-serializable)
 * transactions.
 *
 * Informally, read and write transactions must behave like a ReadWrite Lock -
 * multiple read transactions are allowed in parallel, or one write.
 * Additionally writes from a transaction must appear all at one, atomically.
 *
 */
export declare interface KVStore {
    read(): Promise<KVRead>;
    write(): Promise<KVWrite>;
    close(): Promise<void>;
    closed: boolean;
}

/**
 * @experimental This interface is experimental and might be removed or changed
 * in the future without following semver versioning. Please be cautious.
 */
export declare interface KVWrite extends KVRead {
    put(key: string, value: ReadonlyJSONValue): Promise<void>;
    del(key: string): Promise<void>;
    commit(): Promise<void>;
}

declare type LastInTuple<T extends Relationship> = T extends readonly [infer L] ? L : T extends readonly [unknown, infer L] ? L : T extends readonly [unknown, unknown, infer L] ? L : never;

export declare type LikeOps = 'LIKE' | 'NOT LIKE' | 'ILIKE' | 'NOT ILIKE';

/**
 * Called when the view changes. The received data should be considered
 * immutable. Caller must not modify it. Passed data is valid until next
 * time listener is called.
 */
declare type Listener<T> = (data: Immutable<T>, resultType: ResultType) => void;

declare type Literal = string | number | bigint | boolean;

export declare type LiteralReference = {
    readonly type: 'literal';
    readonly value: LiteralValue;
};

declare function literalUnion<T extends [...Literal[]]>(...literals: T): v_2.Type<T[number]>;

export declare type LiteralValue = string | number | boolean | null | ReadonlyArray<string | number | boolean>;

declare type Location_2 = 'client' | 'server';

export declare type MakeCustomMutatorInterface<S extends Schema, F> = F extends (tx: ClientTransaction<S>, ...args: infer Args) => Promise<void> ? (...args: Args) => PromiseWithServerResult : never;

/**
 * The shape exposed on the `Zero.mutate` instance.
 * The signature of a custom mutator takes a `transaction` as its first arg
 * but the user does not provide this arg when calling the mutator.
 *
 * This utility strips the `tx` arg from the user's custom mutator signatures.
 */
export declare type MakeCustomMutatorInterfaces<S extends Schema, MD extends CustomMutatorDefs> = {
    readonly [NamespaceOrName in keyof MD]: MD[NamespaceOrName] extends (tx: Transaction<S>, ...args: infer Args) => Promise<void> ? (...args: Args) => PromiseWithServerResult : {
        readonly [P in keyof MD[NamespaceOrName]]: MakeCustomMutatorInterface<S, MD[NamespaceOrName][P]>;
    };
};

export declare type MakeEntityQueriesFromSchema<S extends Schema> = {
    readonly [K in keyof S['tables'] & string]: Query<S, K>;
};

/**
 * Returns the name of the IDB database that will be used for a particular Replicache instance.
 * @param name The name of the Replicache instance (i.e., the `name` field of `ReplicacheOptions`).
 * @param schemaVersion The schema version of the database (i.e., the `schemaVersion` field of `ReplicacheOptions`).
 * @returns
 */
export declare function makeIDBName(name: string, schemaVersion?: string): string;

declare type ManyConnection<TSourceField, TDestField, TDest extends TableSchema> = {
    readonly sourceField: TSourceField;
    readonly destField: TDestField;
    readonly destSchema: TDest['name'];
    readonly cardinality: 'many';
};

export declare type MaybePromise<T> = T | Promise<T>;

declare const MutationFailed = "MutationFailed";

declare type MutationFailed = typeof MutationFailed;

declare type MutationOk = v.Infer<typeof mutationOkSchema>;

declare const mutationOkSchema: v.ObjectType<{
    data: v.Optional<ReadonlyJSONValue>;
}, undefined>;

declare const MutationRateLimited = "MutationRateLimited";

declare type MutationRateLimited = typeof MutationRateLimited;

export declare type NamedQuery<TArg extends ReadonlyArray<ReadonlyJSONValue> = ReadonlyArray<ReadonlyJSONValue>, TReturnQuery extends Query<any, any, any> = Query<any, any, any>> = (...args: TArg) => TReturnQuery;

/**
 * There is a new client group due to a another tab loading new code which
 * cannot sync locally with this tab until it updates to the new code. This tab
 * can still sync with the zero-cache.
 */
declare const NewClientGroup = "NewClientGroup";

declare type NewClientGroup = typeof NewClientGroup;

export declare const NOBODY_CAN: never[];

declare type NoCompoundTypeSelector<T extends TableSchema> = Exclude<Selector<T>, JsonSelectors<T> | ArraySelectors<T>>;

/**
 * A row flowing through the pipeline, plus its relationships.
 * Relationships are generated lazily as read.
 */
declare type Node_2 = {
    row: Row_2;
    relationships: Record<string, () => Stream<Node_2>>;
};
export { Node_2 as Node }

declare function not(expression: Condition): Condition;

export declare function number<T extends number = number>(): ColumnBuilder<{
    type: "number";
    optional: false;
    customType: T;
}>;

declare type ObjectShape = Record<string, typeof AbstractType>;

declare type OneConnection<TSourceField, TDestField, TDest extends TableSchema> = {
    readonly sourceField: TSourceField;
    readonly destField: TDestField;
    readonly destSchema: TDest['name'];
    readonly cardinality: 'one';
};

/**
 * Callback function invoked when an error occurs within a Zero instance.
 *
 * @param message - A descriptive error message explaining what went wrong
 * @param rest - Additional context or error details. These are typically:
 *   - Error objects with stack traces
 *   - JSON-serializable data related to the error context
 *   - State information at the time of the error
 */
export declare type OnError = (message: string, ...rest: unknown[]) => void;

/**
 * Type representing the parameter types of the {@link OnError} callback.
 */
export declare type OnErrorParameters = Parameters<OnError>;

declare function or(...conditions: (Condition | undefined)[]): Condition;

export declare type Ordering = readonly OrderPart[];

export declare type OrderOps = '<' | '>' | '<=' | '>=';

/**
 * As in SQL you can have multiple orderings. We don't currently
 * support ordering on anything other than the root query.
 */
export declare type OrderPart = readonly [field: string, direction: 'asc' | 'desc'];

/**
 * An output for an operator. Typically another Operator but can also be
 * the code running the pipeline.
 */
export declare interface Output {
    /**
     * Push incremental changes to data previously received with fetch().
     * Consumers must apply all pushed changes or incremental result will
     * be incorrect.
     * Callers must maintain some invariants for correct operation:
     * - Only add rows which do not already exist (by deep equality).
     * - Only remove rows which do exist (by deep equality).
     */
    push(change: Change): void;
}

export declare type Parameter = v.Infer<typeof parameterReferenceSchema>;

declare type ParameterReference = {
    [toStaticParam](): Parameter;
};

/**
 * A parameter is a value that is not known at the time the query is written
 * and is resolved at runtime.
 *
 * Static parameters refer to something provided by the caller.
 * Static parameters are injected when the query pipeline is built from the AST
 * and do not change for the life of that pipeline.
 *
 * An example static parameter is the current authentication data.
 * When a user is authenticated, queries on the server have access
 * to the user's authentication data in order to evaluate authorization rules.
 * Authentication data doesn't change over the life of a query as a change
 * in auth data would represent a log-in / log-out of the user.
 *
 * AncestorParameters refer to rows encountered while running the query.
 * They are used by subqueries to refer to rows emitted by parent queries.
 */
declare const parameterReferenceSchema: v.ObjectType<Readonly<{
    type: v.Type<"static">;
    anchor: v.Type<"authData" | "preMutationRow">;
    field: v.UnionType<[v.Type<string>, v.ArrayType<v.Type<string>>]>;
}>, undefined>;

declare function parse<T>(value: unknown, schema: v_2.Type<T>, mode?: ParseOptionsMode): T;

/**
 * 'strip' allows unknown properties and removes unknown properties.
 * 'strict' errors if there are unknown properties.
 * 'passthrough' allows unknown properties.
 */
declare type ParseOptionsMode = 'passthrough' | 'strict' | 'strip';

/**
 * This type describes the patch field in a {@link PullResponse} and it is used
 * to describe how to update the Replicache key-value store.
 */
declare type PatchOperation = {
    readonly op: 'put';
    readonly key: string;
    readonly value: ReadonlyJSONValue;
} | {
    readonly op: 'del';
    readonly key: string;
} | {
    readonly op: 'clear';
};

export declare type PermissionRule<TAuthDataShape, TSchema extends Schema, TTable extends keyof TSchema['tables'] & string> = (authData: TAuthDataShape, eb: ExpressionBuilder<TSchema, TTable>) => Condition;

export declare type PermissionsConfig<TAuthDataShape, TSchema extends Schema> = {
    [K in keyof TSchema['tables']]?: {
        row?: AssetPermissions<TAuthDataShape, TSchema, K & string> | undefined;
        cell?: {
            [C in keyof TSchema['tables'][K]['columns']]?: Omit<AssetPermissions<TAuthDataShape, TSchema, K & string>, 'cell'>;
        } | undefined;
    };
};

declare const permissionsConfigSchema: v.ObjectType<{
    tables: v.Type<Record<string, {
        row?: {
            select?: ["allow", Condition][] | undefined;
            insert?: ["allow", Condition][] | undefined;
            update?: {
                preMutation?: ["allow", Condition][] | undefined;
                postMutation?: ["allow", Condition][] | undefined;
            } | undefined;
            delete?: ["allow", Condition][] | undefined;
        } | undefined;
        cell?: Record<string, {
            select?: ["allow", Condition][] | undefined;
            insert?: ["allow", Condition][] | undefined;
            update?: {
                preMutation?: ["allow", Condition][] | undefined;
                postMutation?: ["allow", Condition][] | undefined;
            } | undefined;
            delete?: ["allow", Condition][] | undefined;
        }> | undefined;
    }>>;
}, undefined>;

declare const policySchema: v.ArrayType<v.TupleType<[v.Type<"allow">, v.Type<Condition>]>>;

declare type PreloadOptions = {
    /**
     * Time To Live. This is the amount of time to keep the rows associated with
     * this query after {@linkcode cleanup} has been called.
     */
    ttl?: TTL | undefined;
};

declare type Prev = [-1, 0, 1, 2, 3, 4, 5, 6];

declare type PreviousSchema<TSource extends TableSchema, K extends number, TDests extends TableSchema[]> = K extends 0 ? TSource : TDests[Prev[K]];

declare type PrimaryKey = v.Infer<typeof primaryKeySchema>;

declare type PrimaryKeyFields<S extends TableSchema> = {
    [K in Extract<S['primaryKey'][number], keyof S['columns']>]: SchemaValueToTSType<S['columns'][K]>;
};

declare const primaryKeySchema: v.Type<readonly [string, ...string[]]>;

declare type Primitive = undefined | null | boolean | string | number | symbol | bigint;

export declare type PromiseWithServerResult = {
    client: Promise<void>;
    server: Promise<MutationOk>;
};

/**
 * Puller is the function type used to do the fetch part of a pull.
 *
 * Puller needs to support dealing with pull request of version 0 and 1. Version
 * 0 is used when doing mutation recovery of old clients. If a
 * {@link PullRequestV1} is passed in the n a {@link PullerResultV1} should
 * be returned. We do a runtime assert to make this is the case.
 *
 * If you do not support old clients you can just throw if `pullVersion` is `0`,
 */
declare type Puller = (requestBody: PullRequest, requestID: string) => Promise<PullerResult>;

declare type PullerResult = PullerResultV1;

declare type PullerResultV1 = {
    response?: PullResponseV1 | undefined;
    httpRequestInfo: HTTPRequestInfo;
};

/**
 * The JSON value used as the body when doing a POST to the [pull
 * endpoint](/reference/server-pull).
 */
declare type PullRequest = PullRequestV1;

/**
 * The JSON value used as the body when doing a POST to the [pull
 * endpoint](/reference/server-pull).
 */
declare type PullRequestV1 = {
    pullVersion: 1;
    schemaVersion: string;
    profileID: string;
    cookie: Cookie;
    clientGroupID: ClientGroupID;
};

/**
 * The shape of a pull response under normal circumstances.
 */
declare type PullResponseOKV1 = {
    cookie: Cookie;
    lastMutationIDChanges: Record<ClientID, number>;
    patch: PatchOperation[];
};

/**
 * PullResponse defines the shape and type of the response of a pull. This is
 * the JSON you should return from your pull server endpoint.
 */
declare type PullResponseV1 = PullResponseOKV1 | ClientStateNotFoundResponse | VersionNotSupportedResponse;

export declare type PullRow<TTable extends string, TSchema extends Schema> = {
    readonly [K in keyof PullTableSchema<TTable, TSchema>['columns']]: SchemaValueToTSType<PullTableSchema<TTable, TSchema>['columns'][K]>;
};

declare type PullTableSchema<TTable extends string, TSchemas extends Schema> = TSchemas['tables'][TTable];

export declare function queries<TQueries extends {
    [K in keyof TQueries]: TQueries[K] extends NamedQuery<infer TArgs, Query<any, any, any>> ? TArgs extends ReadonlyArray<ReadonlyJSONValue> ? NamedQuery<TArgs, Query<any, any, any>> : never : never;
}>(queries: TQueries): TQueries;

export declare function queriesWithContext<TContext, TQueries extends {
    [K in keyof TQueries]: TQueries[K] extends ContextualizedNamedQuery<TContext, infer TArgs, Query<any, any, any>> ? TArgs extends ReadonlyArray<ReadonlyJSONValue> ? ContextualizedNamedQuery<TContext, TArgs, Query<any, any, any>> : never : never;
}>(queries: TQueries): TQueries;

/**
 * A hybrid query that runs on both client and server.
 * Results are returned immediately from the client followed by authoritative
 * results from the server.
 *
 * Queries are transactional in that all queries update at once when a new transaction
 * has been committed on the client or server. No query results will reflect stale state.
 *
 * A query can be:
 * - {@linkcode materialize | materialize}
 * - awaited (`then`/{@linkcode run})
 * - {@linkcode preload | preloaded}
 *
 * The normal way to use a query would be through your UI framework's bindings (e.g., useQuery(q))
 * or within a custom mutator.
 *
 * `materialize` and `run/then` are provided for more advanced use cases.
 * Remember that any `view` returned by `materialize` must be destroyed.
 *
 * A query can be run as a 1-shot query by awaiting it. E.g.,
 *
 * ```ts
 * const result = await z.query.issue.limit(10);
 * ```
 *
 * For more information on how to use queries, see the documentation:
 * https://zero.rocicorp.dev/docs/reading-data
 *
 * @typeParam TSchema The database schema type extending ZeroSchema
 * @typeParam TTable The name of the table being queried, must be a key of TSchema['tables']
 * @typeParam TReturn The return type of the query, defaults to PullRow<TTable, TSchema>
 */
export declare interface Query<TSchema extends Schema, TTable extends keyof TSchema['tables'] & string, TReturn = PullRow<TTable, TSchema>> extends PromiseLike<HumanReadable<TReturn>> {
    /**
     * Format is used to specify the shape of the query results. This is used by
     * {@linkcode one} and it also describes the shape when using
     * {@linkcode related}.
     */
    readonly format: Format;
    /**
     * A string that uniquely identifies this query. This can be used to determine
     * if two queries are the same.
     *
     * The hash of a custom query, on the client, is the hash of its AST.
     * The hash of a custom query, on the server, is the hash of its name and args.
     *
     * The first allows many client-side queries to be pinned to the same backend query.
     * The second ensures we do not invoke a named query on the backend more than once for the same `name:arg` pairing.
     *
     * If the query.hash was of `name:args` then `useQuery` would de-dupe
     * queries with divergent ASTs.
     *
     * QueryManager will hash based on `name:args` since it is speaking with
     * the server which tracks queries by `name:args`.
     */
    hash(): string;
    readonly ast: AST;
    readonly customQueryID: CustomQueryID | undefined;
    nameAndArgs(name: string, args: ReadonlyArray<ReadonlyJSONValue>): Query<TSchema, TTable, TReturn>;
    delegate(delegate: QueryDelegate): Query<TSchema, TTable, TReturn>;
    /**
     * Related is used to add a related query to the current query. This is used
     * for subqueries and joins. These relationships are defined in the
     * relationships section of the schema. The result of the query will
     * include the related rows in the result set as a sub object of the row.
     *
     * ```typescript
     * const row = await z.query.users
     *   .related('posts');
     * // {
     * //   id: '1',
     * //   posts: [
     * //     ...
     * //   ]
     * // }
     * ```
     * If you want to add a subquery to the related query, you can do so by
     * providing a callback function that receives the related query as an argument.
     *
     * ```typescript
     * const row = await z.query.users
     *   .related('posts', q => q.where('published', true));
     * // {
     * //   id: '1',
     * //   posts: [
     * //     {published: true, ...},
     * //     ...
     * //   ]
     * // }
     * ```
     *
     * @param relationship The name of the relationship
     */
    related<TRelationship extends AvailableRelationships<TTable, TSchema>>(relationship: TRelationship): Query<TSchema, TTable, AddSubreturn<TReturn, DestRow<TTable, TSchema, TRelationship>, TRelationship>>;
    related<TRelationship extends AvailableRelationships<TTable, TSchema>, TSub extends Query<TSchema, string, any>>(relationship: TRelationship, cb: (q: Query<TSchema, DestTableName<TTable, TSchema, TRelationship>, DestRow<TTable, TSchema, TRelationship>>) => TSub): Query<TSchema, TTable, AddSubreturn<TReturn, TSub extends Query<TSchema, string, infer TSubReturn> ? TSubReturn : never, TRelationship>>;
    /**
     * Represents a condition to filter the query results.
     *
     * @param field The column name to filter on.
     * @param op The operator to use for filtering.
     * @param value The value to compare against.
     *
     * @returns A new query instance with the applied filter.
     *
     * @example
     *
     * ```typescript
     * const query = db.query('users')
     *   .where('age', '>', 18)
     *   .where('name', 'LIKE', '%John%');
     * ```
     */
    where<TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>, TOperator extends SimpleOperator>(field: TSelector, op: TOperator, value: GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, TOperator> | ParameterReference): Query<TSchema, TTable, TReturn>;
    /**
     * Represents a condition to filter the query results.
     *
     * This overload is used when the operator is '='.
     *
     * @param field The column name to filter on.
     * @param value The value to compare against.
     *
     * @returns A new query instance with the applied filter.
     *
     * @example
     * ```typescript
     * const query = db.query('users')
     *  .where('age', 18)
     * ```
     */
    where<TSelector extends NoCompoundTypeSelector<PullTableSchema<TTable, TSchema>>>(field: TSelector, value: GetFilterType<PullTableSchema<TTable, TSchema>, TSelector, '='> | ParameterReference): Query<TSchema, TTable, TReturn>;
    /**
     * Represents a condition to filter the query results.
     *
     * @param expressionFactory A function that takes a query builder and returns an expression.
     *
     * @returns A new query instance with the applied filter.
     *
     * @example
     * ```typescript
     * const query = db.query('users')
     *   .where(({cmp, or}) => or(cmp('age', '>', 18), cmp('name', 'LIKE', '%John%')));
     * ```
     */
    where(expressionFactory: ExpressionFactory<TSchema, TTable>): Query<TSchema, TTable, TReturn>;
    whereExists(relationship: AvailableRelationships<TTable, TSchema>): Query<TSchema, TTable, TReturn>;
    whereExists<TRelationship extends AvailableRelationships<TTable, TSchema>>(relationship: TRelationship, cb: (q: Query<TSchema, DestTableName<TTable, TSchema, TRelationship>>) => Query<TSchema, string>): Query<TSchema, TTable, TReturn>;
    /**
     * Skips the rows of the query until row matches the given row. If opts is
     * provided, it determines whether the match is inclusive.
     *
     * @param row The row to start from. This is a partial row object and only the provided
     *            fields will be used for the comparison.
     * @param opts Optional options object that specifies whether the match is inclusive.
     *             If `inclusive` is true, the row will be included in the result.
     *             If `inclusive` is false, the row will be excluded from the result and the result
     *             will start from the next row.
     *
     * @returns A new query instance with the applied start condition.
     */
    start(row: Partial<PullRow<TTable, TSchema>>, opts?: {
        inclusive: boolean;
    } | undefined): Query<TSchema, TTable, TReturn>;
    /**
     * Limits the number of rows returned by the query.
     * @param limit The maximum number of rows to return.
     *
     * @returns A new query instance with the applied limit.
     */
    limit(limit: number): Query<TSchema, TTable, TReturn>;
    /**
     * Orders the results by a specified column. If multiple orderings are
     * specified, the results will be ordered by the first column, then the
     * second column, and so on.
     *
     * @param field The column name to order by.
     * @param direction The direction to order the results (ascending or descending).
     *
     * @returns A new query instance with the applied order.
     */
    orderBy<TSelector extends Selector<PullTableSchema<TTable, TSchema>>>(field: TSelector, direction: 'asc' | 'desc'): Query<TSchema, TTable, TReturn>;
    /**
     * Limits the number of rows returned by the query to a single row and then
     * unpacks the result so that you do not get an array of rows but a single
     * row. This is useful when you expect only one row to be returned and want to
     * work with the row directly.
     *
     * If the query returns no rows, the result will be `undefined`.
     *
     * @returns A new query instance with the applied limit to one row.
     */
    one(): Query<TSchema, TTable, TReturn | undefined>;
    /**
     * Creates a materialized view of the query. This is a view that will be kept
     * in memory and updated as the query results change.
     *
     * Most of the time you will want to use the `useQuery` hook or the
     * `run`/`then` method to get the results of a query. This method is only
     * needed if you want to access to lower level APIs of the view.
     *
     * @param ttl Time To Live. This is the amount of time to keep the rows
     *            associated with this query after `TypedView.destroy`
     *            has been called.
     */
    materialize(ttl?: TTL): TypedView<HumanReadable<TReturn>>;
    /**
     * Creates a custom materialized view using a provided factory function. This
     * allows framework-specific bindings (like SolidJS, Vue, etc.) to create
     * optimized views.
     *
     * @param factory A function that creates a custom view implementation
     * @param ttl Optional Time To Live for the view's data after destruction
     * @returns A custom view instance of type {@linkcode T}
     *
     * @example
     * ```ts
     * const view = query.materialize(createSolidViewFactory, '1m');
     * ```
     */
    materialize<T>(factory: ViewFactory<TSchema, TTable, TReturn, T>, ttl?: TTL): T;
    /**
     * Executes the query and returns the result once. The `options` parameter
     * specifies whether to wait for complete results or return immediately,
     * and the time to live for the query.
     *
     * - `{type: 'unknown'}`: Returns a snapshot of the data immediately.
     * - `{type: 'complete'}`: Waits for the latest, complete results from the server.
     *
     * By default, `run` uses `{type: 'unknown'}` to avoid waiting for the server.
     *
     * `Query` implements `PromiseLike`, and calling `then` on it will invoke `run`
     * with the default behavior (`unknown`).
     *
     * @param options Options to control the result type.
     * @param options.type The type of result to return.
     * @param options.ttl Time To Live. This is the amount of time to keep the rows
     *                  associated with this query after the returned promise has
     *                  resolved.
     * @returns A promise resolving to the query result.
     *
     * @example
     * ```js
     * const result = await query.run({type: 'complete', ttl: '1m'});
     * ```
     */
    run(options?: RunOptions): Promise<HumanReadable<TReturn>>;
    /**
     * Preload loads the data into the clients cache without keeping it in memory.
     * This is useful for preloading data that will be used later.
     *
     * @param options Options for preloading the query.
     * @param options.ttl Time To Live. This is the amount of time to keep the rows
     *                  associated with this query after {@linkcode cleanup} has
     *                  been called.
     */
    preload(options?: PreloadOptions): {
        cleanup: () => void;
        complete: Promise<void>;
    };
}

declare interface Queryable {
    query: (query: string, args: unknown[]) => Promise<Iterable<Row_3>>;
}

declare interface QueryDelegate extends BuilderDelegate {
    addServerQuery(ast: AST, ttl: TTL, gotCallback?: GotCallback | undefined): () => void;
    addCustomQuery(customQueryID: CustomQueryID, ttl: TTL, gotCallback?: GotCallback | undefined): () => void;
    updateServerQuery(ast: AST, ttl: TTL): void;
    updateCustomQuery(customQueryID: CustomQueryID, ttl: TTL): void;
    flushQueryChanges(): void;
    onTransactionCommit(cb: CommitListener): () => void;
    batchViewUpdates<T>(applyViewUpdates: () => T): T;
    onQueryMaterialized(hash: string, ast: AST, duration: number): void;
    /**
     * Asserts that the `RunOptions` provided to the `run` method are supported in
     * this context. For example, in a custom mutator, the `{type: 'complete'}`
     * option is not supported and this will throw.
     */
    assertValidRunOptions(options?: RunOptions): void;
    /**
     * Client queries start off as false (`unknown`) and are set to true when the
     * server sends the gotQueries message.
     *
     * For things like ZQLite the default is true (aka `complete`) because the
     * data is always available.
     */
    readonly defaultQueryComplete: boolean;
}

/**
 * Shallowly marks the schema as readonly.
 */
declare function readonly<T extends v_2.Type>(t: T): v_2.Type<Readonly<v_2.Infer<T>>>;

declare function readonlyArray<T extends v_2.Type>(t: T): v_2.Type<readonly v_2.Infer<T>[]>;

/** Like {@link JSONObject} but deeply readonly */
export declare type ReadonlyJSONObject = {
    readonly [key: string]: ReadonlyJSONValue | undefined;
};

/** Like {@link JSONValue} but deeply readonly */
export declare type ReadonlyJSONValue = null | string | boolean | number | ReadonlyArray<ReadonlyJSONValue> | ReadonlyJSONObject;

declare function readonlyObject<T extends Record<string, v_2.Type | v_2.Optional>>(t: T): v_2.ObjectType<Readonly<T>, undefined>;

declare function readonlyRecord<T extends v_2.Type>(t: T): v_2.Type<Readonly<Record<string, v_2.Infer<T>>>>;

declare const Rebalance = "Rebalance";

declare type Rebalance = typeof Rebalance;

declare const Rehome = "Rehome";

declare type Rehome = typeof Rehome;

declare type Relationship = readonly [Connection] | readonly [Connection, Connection];

declare type Relationships = {
    name: string;
    relationships: Record<string, Relationship>;
};

export declare function relationships<TSource extends TableSchema, TRelationships extends Record<string, Relationship>>(table: TableBuilderWithColumns<TSource>, cb: (connects: {
    many: <TDests extends TableSchema[], TSourceFields extends {
        [K in keyof TDests]: (keyof PreviousSchema<TSource, K & number, TDests>['columns'] & string)[];
    }, TDestFields extends {
        [K in keyof TDests]: (keyof TDests[K]['columns'] & string)[];
    }>(...args: {
        [K in keyof TDests]: ConnectArg<TSourceFields[K], TDestFields[K], TDests[K]>;
    }) => {
        [K in keyof TDests]: ManyConnection<TSourceFields[K], TDestFields[K], TDests[K]>;
    };
    one: <TDests extends TableSchema[], TSourceFields extends {
        [K in keyof TDests]: (keyof PreviousSchema<TSource, K & number, TDests>['columns'] & string)[];
    }, TDestFields extends {
        [K in keyof TDests]: (keyof TDests[K]['columns'] & string)[];
    }>(...args: {
        [K in keyof TDests]: ConnectArg<TSourceFields[K], TDestFields[K], TDests[K]>;
    }) => {
        [K in keyof TDests]: OneConnection<TSourceFields[K], TDestFields[K], TDests[K]>;
    };
}) => TRelationships): {
    name: TSource['name'];
    relationships: TRelationships;
};

declare type RelationshipsSchema = {
    readonly [name: string]: Relationship;
};

/**
 * This interface is used so that we can release the lock when the transaction
 * is done.
 *
 * @experimental This interface is experimental and might be removed or changed
 * in the future without following semver versioning. Please be cautious.
 */
declare interface Release {
    release(): void;
}

/**
 * Represents a node (and all its children) getting removed from the result.
 */
declare type RemoveChange = {
    type: 'remove';
    node: Node_2;
};

declare type RemoveViewChange = {
    type: 'remove';
    node: Node_2;
};

declare type Result<T> = {
    ok: true;
    value: T;
} | {
    ok: false;
    error: string;
};

export declare type ResultType = 'unknown' | 'complete';

export declare type Row<T extends TableSchema | Query<Schema, string, any>> = T extends TableSchema ? {
    readonly [K in keyof T['columns']]: SchemaValueToTSType<T['columns'][K]>;
} : T extends Query<Schema, string, infer TReturn> ? TReturn : never;

/**
 * A Row is represented as a JS Object.
 *
 * We do everything in IVM as loosely typed values because these pipelines are
 * going to be constructed at runtime by other code, so type-safety can't buy us
 * anything.
 *
 * Also since the calling code on the client ultimately wants objects to work
 * with we end up with a lot less copies by using objects throughout.
 */
declare type Row_2 = v.Infer<typeof rowSchema>;

declare interface Row_3 {
    [column: string]: unknown;
}

declare type RowOnlyNode = {
    row: Row_2;
};

declare const rowSchema: v.Type<Readonly<Record<string, ReadonlyJSONValue | undefined>>>;

declare const ruleSchema: v.TupleType<[v.Type<"allow">, v.Type<Condition>]>;

/**
 * The kind of results we want to wait for when using {@linkcode run} on {@linkcode Query}.
 *
 * `unknown` means we don't want to wait for the server to return results. The result is a
 * snapshot of the data at the time the query was run.
 *
 * `complete` means we want to ensure that we have the latest result from the server. The
 * result is a complete and up-to-date view of the data. In some cases this means that we
 * have to wait for the server to return results. To ensure that we have the result for
 * this query you can preload it before calling run. See {@link preload}.
 *
 * By default, `run` uses `{type: 'unknown'}` to avoid waiting for the server.
 *
 * The `ttl` option is used to specify the time to live for the query. This is the amount of
 * time to keep the rows associated with this query after the promise has resolved.
 */
export declare type RunOptions = {
    type: 'unknown' | 'complete';
    ttl?: TTL;
};

export declare type Schema = {
    readonly tables: {
        readonly [table: string]: TableSchema;
    };
    readonly relationships: {
        readonly [table: string]: RelationshipsSchema;
    };
};

declare type SchemaCRUD<S extends Schema> = {
    [Table in keyof S['tables']]: TableCRUD<S['tables'][Table]>;
};

export declare type SchemaQuery<S extends Schema> = {
    readonly [K in keyof S['tables'] & string]: Query<S, K>;
};

/**
 * `related` calls need to know what the available relationships are.
 * The `schema` type encodes this information.
 */
export declare type SchemaValue<T = unknown> = {
    type: ValueType;
    serverName?: string | undefined;
    optional?: boolean | undefined;
} | EnumSchemaValue<T> | SchemaValueWithCustomType<T>;

/**
 * Given a schema value, return the TypeScript type.
 *
 * This allows us to create the correct return type for a
 * query that has a selection.
 */
declare type SchemaValueToTSType<T extends SchemaValue | ValueType> = T extends ValueType ? TypeNameToTypeMap[T] : T extends {
    optional: true;
} ? (T extends SchemaValueWithCustomType<infer V> ? V : TypeNameToTypeMap[ColumnTypeName<T>]) | null : T extends SchemaValueWithCustomType<infer V> ? V : TypeNameToTypeMap[ColumnTypeName<T>];

export declare type SchemaValueWithCustomType<T> = {
    type: ValueType;
    serverName?: string | undefined;
    optional?: boolean;
    customType: T;
};

declare const SchemaVersionNotSupported = "SchemaVersionNotSupported";

declare type SchemaVersionNotSupported = typeof SchemaVersionNotSupported;

/**
 * This client was unable to connect to the zero-cache because it is using a
 * schema version (see {@codelink Schema}) that the zero-cache does not support.
 */
declare const SchemaVersionNotSupported_2 = "SchemaVersionNotSupported";

declare type SchemaVersionNotSupported_2 = typeof SchemaVersionNotSupported_2;

declare type Selector<E extends TableSchema> = keyof E['columns'];

declare const ServerOverloaded = "ServerOverloaded";

declare type ServerOverloaded = typeof ServerOverloaded;

export declare interface ServerTransaction<S extends Schema, TWrappedTransaction> extends TransactionBase<S> {
    readonly location: 'server';
    readonly reason: 'authoritative';
    readonly dbTransaction: DBTransaction<TWrappedTransaction>;
}

export declare type SimpleCondition = {
    readonly type: 'simple';
    readonly op: SimpleOperator;
    readonly left: ValuePosition;
    /**
     * `null` is absent since we do not have an `IS` or `IS NOT`
     * operator defined and `null != null` in SQL.
     */
    readonly right: Exclude<ValuePosition, ColumnReference>;
};

export declare type SimpleOperator = EqualityOps | OrderOps | LikeOps | InOps;

/**
 * A source is an input that serves as the root data source of the pipeline.
 * Sources have multiple outputs. To add an output, call `connect()`, then
 * hook yourself up to the returned Connector, like:
 *
 * ```ts
 * class MyOperator implements Output {
 *   constructor(input: Input) {
 *     input.setOutput(this);
 *   }
 *
 *   push(change: Change): void {
 *     // Handle change
 *   }
 * }
 *
 * const connection = source.connect(ordering);
 * const myOperator = new MyOperator(connection);
 * ```
 */
declare interface Source {
    /**
     * Creates an input that an operator can connect to. To free resources used
     * by connection, downstream operators call `destroy()` on the returned
     * input.
     *
     * @param sort The ordering of the rows. Source must return rows in this
     * order.
     * @param filters Filters to apply to the source.
     * @param splitEditKeys If an edit change modifies the values of any of the
     *   keys in splitEditKeys, the source should split the edit change into
     *   a remove of the old row followed by an add of the new row.
     */
    connect(sort: Ordering, filters?: Condition | undefined, splitEditKeys?: Set<string> | undefined): SourceInput;
    /**
     * Pushes a change into the source and into all connected outputs.
     */
    push(change: SourceChange | SourceChangeSet): void;
    /**
     * Pushes a change into the source.
     * Iterating the returned iterator will push the
     * change into one connected input at a time.
     *
     * Once the iterator is exhausted, the change will
     * have been pushed into all connected inputs and
     * committed to the source.
     */
    genPush(change: SourceChange): Iterable<void>;
}

declare type SourceChange = SourceChangeAdd | SourceChangeRemove | SourceChangeEdit;

declare type SourceChangeAdd = {
    type: 'add';
    row: Row_2;
};

declare type SourceChangeEdit = {
    type: 'edit';
    row: Row_2;
    oldRow: Row_2;
};

declare type SourceChangeRemove = {
    type: 'remove';
    row: Row_2;
};

declare type SourceChangeSet = {
    type: 'set';
    row: Row_2;
};

declare interface SourceInput extends Input {
    readonly fullyAppliedFilters: boolean;
}

/**
 * Information about the nodes output by an operator.
 */
declare type SourceSchema = {
    readonly tableName: string;
    readonly columns: Record<string, SchemaValue>;
    readonly primaryKey: PrimaryKey;
    readonly relationships: {
        [key: string]: SourceSchema;
    };
    readonly isHidden: boolean;
    readonly system: System;
    readonly compareRows: Comparator;
    readonly sort: Ordering;
};

declare type Start = {
    readonly row: Row_2;
    readonly basis: 'at' | 'after';
};

/**
 * Operators get access to storage that they can store their internal
 * state in.
 */
declare interface Storage_2 {
    set(key: string, value: JSONValue): void;
    get(key: string, def?: JSONValue): JSONValue | undefined;
    /**
     * If options is not specified, defaults to scanning all entries.
     */
    scan(options?: {
        prefix: string;
    }): Stream<[string, JSONValue]>;
    del(key: string): void;
}

/**
 * Provider for creating and deleting {@link Store} instances.
 *
 */
declare type StoreProvider = {
    create: CreateKVStore;
    drop: DropStore;
};

/**
 * streams are lazy forward-only iterables.
 * Once a stream reaches the end it can't be restarted.
 * They are iterable, not iterator, so that they can be used in for-each,
 * and so that we know when consumer has stopped iterating the stream. This allows us
 * to clean up resources like sql statements.
 */
export declare type Stream<T> = Iterable<T>;

export declare function string<T extends string = string>(): ColumnBuilder<{
    type: "string";
    optional: false;
    customType: T;
}>;

declare type System = 'permissions' | 'client' | 'test';

export declare function table<TName extends string>(name: TName): TableBuilder<{
    name: TName;
    columns: {};
    primaryKey: PrimaryKey;
}>;

declare class TableBuilder<TShape extends TableSchema> {
    #private;
    constructor(schema: TShape);
    from<ServerName extends string>(serverName: ServerName): TableBuilder<TShape>;
    columns<const TColumns extends Record<string, ColumnBuilder<SchemaValue>>>(columns: TColumns): TableBuilderWithColumns<{
        name: TShape['name'];
        columns: {
            [K in keyof TColumns]: TColumns[K]['schema'];
        };
        primaryKey: TShape['primaryKey'];
    }>;
}

export declare class TableBuilderWithColumns<TShape extends TableSchema> {
    #private;
    constructor(schema: TShape);
    primaryKey<TPKColNames extends (keyof TShape['columns'])[]>(...pkColumnNames: TPKColNames): TableBuilderWithColumns<TShape & {
        primaryKey: TPKColNames;
    }>;
    get schema(): TShape;
    build(): TShape;
}

declare type TableCRUD<S extends TableSchema> = {
    /**
     * Writes a row if a row with the same primary key doesn't already exists.
     * Non-primary-key fields that are 'optional' can be omitted or set to
     * `undefined`. Such fields will be assigned the value `null` optimistically
     * and then the default value as defined by the server.
     */
    insert: (value: InsertValue<S>) => Promise<void>;
    /**
     * Writes a row unconditionally, overwriting any existing row with the same
     * primary key. Non-primary-key fields that are 'optional' can be omitted or
     * set to `undefined`. Such fields will be assigned the value `null`
     * optimistically and then the default value as defined by the server.
     */
    upsert: (value: UpsertValue<S>) => Promise<void>;
    /**
     * Updates a row with the same primary key. If no such row exists, this
     * function does nothing. All non-primary-key fields can be omitted or set to
     * `undefined`. Such fields will be left unchanged from previous value.
     */
    update: (value: UpdateValue<S>) => Promise<void>;
    /**
     * Deletes the row with the specified primary key. If no such row exists, this
     * function does nothing.
     */
    delete: (id: DeleteID<S>) => Promise<void>;
};

/**
 * This is the type of the generated mutate.<name>.<verb> function.
 */
export declare type TableMutator<S extends TableSchema> = {
    /**
     * Writes a row if a row with the same primary key doesn't already exists.
     * Non-primary-key fields that are 'optional' can be omitted or set to
     * `undefined`. Such fields will be assigned the value `null` optimistically
     * and then the default value as defined by the server.
     */
    insert: (value: InsertValue<S>) => Promise<void>;
    /**
     * Writes a row unconditionally, overwriting any existing row with the same
     * primary key. Non-primary-key fields that are 'optional' can be omitted or
     * set to `undefined`. Such fields will be assigned the value `null`
     * optimistically and then the default value as defined by the server.
     */
    upsert: (value: UpsertValue<S>) => Promise<void>;
    /**
     * Updates a row with the same primary key. If no such row exists, this
     * function does nothing. All non-primary-key fields can be omitted or set to
     * `undefined`. Such fields will be left unchanged from previous value.
     */
    update: (value: UpdateValue<S>) => Promise<void>;
    /**
     * Deletes the row with the specified primary key. If no such row exists, this
     * function does nothing.
     */
    delete: (id: DeleteID<S>) => Promise<void>;
};

export declare type TableSchema = {
    readonly name: string;
    readonly serverName?: string | undefined;
    readonly columns: Record<string, SchemaValue>;
    readonly primaryKey: PrimaryKey;
};

declare function test<T>(value: unknown, schema: v_2.Type<T>, mode?: ParseOptionsMode): Result<T>;

/**
 * Similar to {@link test} but works for AbstractTypes such as Optional.
 * This is for advanced usage. Prefer {@link test} unless you really need
 * to operate directly on an Optional field.
 */
declare function testOptional<T>(value: unknown, schema: v_2.Type<T> | v_2.Optional<T>, mode?: ParseOptionsMode): Result<T | undefined>;

declare type TimeUnit = 's' | 'm' | 'h' | 'd' | 'y';

declare const toStaticParam: unique symbol;

export declare type Transaction<S extends Schema, TWrappedTransaction = unknown> = ServerTransaction<S, TWrappedTransaction> | ClientTransaction<S>;

declare interface TransactionBase<S extends Schema> {
    readonly location: Location_2;
    readonly clientID: ClientID_2;
    /**
     * The ID of the mutation that is being applied.
     */
    readonly mutationID: number;
    /**
     * The reason for the transaction.
     */
    readonly reason: TransactionReason;
    readonly mutate: SchemaCRUD<S>;
    readonly query: SchemaQuery<S>;
}

/**
 * This error is thrown when you try to call methods on a closed transaction.
 */
export declare class TransactionClosedError extends Error {
    constructor();
}

declare type TransactionReason = 'optimistic' | 'rebase' | 'authoritative';

export declare type TransformRequestBody = v.Infer<typeof transformRequestBodySchema>;

declare const transformRequestBodySchema: v.ArrayType<v.ObjectType<{
    id: v.Type<string>;
    name: v.Type<string>;
    args: v.Type<readonly ReadonlyJSONValue[]>;
}, undefined>>;

export declare type TransformRequestMessage = v.Infer<typeof transformRequestMessageSchema>;

export declare const transformRequestMessageSchema: v.TupleType<[v.Type<"transform">, v.ArrayType<v.ObjectType<{
    id: v.Type<string>;
    name: v.Type<string>;
    args: v.Type<readonly ReadonlyJSONValue[]>;
}, undefined>>]>;

export declare type TransformResponseBody = v.Infer<typeof transformResponseBodySchema>;

declare const transformResponseBodySchema: v.ArrayType<v.UnionType<[v.ObjectType<{
    id: v.Type<string>;
    name: v.Type<string>;
    ast: v.Type<AST>;
}, undefined>, v.ObjectType<{
    error: v.Type<"app">;
    id: v.Type<string>;
    name: v.Type<string>;
    details: v.Type<ReadonlyJSONValue>;
}, undefined>]>>;

export declare type TransformResponseMessage = v.Infer<typeof transformResponseMessageSchema>;

export declare const transformResponseMessageSchema: v.TupleType<[v.Type<"transformed">, v.ArrayType<v.UnionType<[v.ObjectType<{
    id: v.Type<string>;
    name: v.Type<string>;
    ast: v.Type<AST>;
}, undefined>, v.ObjectType<{
    error: v.Type<"app">;
    id: v.Type<string>;
    name: v.Type<string>;
    details: v.Type<ReadonlyJSONValue>;
}, undefined>]>>]>;

/**
 * Time To Live. This is used for query expiration.
 * - `forever` means the query will never expire.
 * - `none` means the query will expire immediately.
 * - A number means the query will expire after that many milliseconds.
 * - A negative number means the query will never expire, this is same as 'forever'.
 * - A string like `1s` means the query will expire after that many seconds.
 * - A string like `1m` means the query will expire after that many minutes.
 * - A string like `1h` means the query will expire after that many hours.
 * - A string like `1d` means the query will expire after that many days.
 * - A string like `1y` means the query will expire after that many years.
 */
export declare type TTL = `${number}${TimeUnit}` | 'forever' | 'none' | number;

export declare type TypedView<T> = {
    addListener(listener: Listener<T>): () => void;
    destroy(): void;
    updateTTL(ttl: TTL): void;
    readonly data: T;
};

declare type TypeNameToTypeMap = {
    string: string;
    number: number;
    boolean: boolean;
    null: null;
    json: any;
};

declare const Unauthorized = "Unauthorized";

declare type Unauthorized = typeof Unauthorized;

export declare type UpdateNeededReason = {
    type: UpdateNeededReasonType.NewClientGroup;
} | {
    type: UpdateNeededReasonType.VersionNotSupported;
} | {
    type: UpdateNeededReasonType.SchemaVersionNotSupported;
};

declare namespace UpdateNeededReasonType {
    export {
        NewClientGroup,
        VersionNotSupported_2 as VersionNotSupported,
        SchemaVersionNotSupported_2 as SchemaVersionNotSupported
    }
}
export { UpdateNeededReasonType }

export declare type UpdateValue<S extends TableSchema> = Expand<PrimaryKeyFields<S> & {
    [K in keyof S['columns']]?: SchemaValueToTSType<S['columns'][K]> | undefined;
}>;

export declare type UpsertValue<S extends TableSchema> = InsertValue<S>;

declare type UserMutateParams = v.Infer<typeof userQueryMutateParamsSchema>;

declare const userQueryMutateParamsSchema: v.ObjectType<{
    /**
     * A client driven URL to send queries or mutations to.
     * This URL must match one of the URLs set in the zero config.
     *
     * E.g., Given the following environment variable:
     * ZERO_QUERY_URL=[https://*.example.com/query]
     *
     * Then this URL could be:
     * https://myapp.example.com/query
     */
    url: v.Optional<string>;
    queryParams: v.Optional<Record<string, string>>;
}, undefined>;

declare type UserQueryParams = v.Infer<typeof userQueryMutateParamsSchema>;

declare namespace v {
    export {
        parse,
        is,
        assert,
        test,
        testOptional,
        readonly,
        readonlyObject,
        readonlyArray,
        readonlyRecord,
        instanceOfAbstractType,
        deepPartial,
        literalUnion,
        ParseOptionsMode
    }
}

/**
 * The data types that Zero can represent are limited by two things:
 *
 * 1. The underlying Replicache sync layer currently can only represent JSON
 *    types. This could possibly be expanded in the future, but we do want to be
 *    careful of adding encoding overhead. By using JSON, we are taking
 *    advantage of IndexedDB’s fast native JSValue [de]serialization which has
 *    historically been a perf advantage for us.
 *
 * 2. IDs in Zero need to be comparable because we use them for sorting and row
 *    identity. We could expand the set of allowed value types (to include,
 *    i.e., Objects) but we would then need to restrict IDs to only comparable
 *    types.
 *
 * These two facts leave us with the following allowed types. Zero's replication
 * layer must convert other types into these for tables to be used with Zero.
 *
 * For developer convenience we also allow `undefined`, which we treat
 * equivalently to `null`.
 */
declare type Value = v.Infer<typeof valueSchema>;

export declare type ValuePosition = LiteralReference | Parameter | ColumnReference;

declare const valueSchema: v.UnionType<[v.Type<ReadonlyJSONValue>, v.Type<undefined>]>;

export declare type ValueType = 'string' | 'number' | 'boolean' | 'null' | 'json';

declare const VersionNotSupported = "VersionNotSupported";

declare type VersionNotSupported = typeof VersionNotSupported;

/**
 * This client was unable to connect to the zero-cache because it is using a
 * protocol version that the zero-cache does not support.
 */
declare const VersionNotSupported_2 = "VersionNotSupported";

declare type VersionNotSupported_2 = typeof VersionNotSupported_2;

/**
 * The server endpoint may respond with a `VersionNotSupported` error if it does
 * not know how to handle the pull, push or schema version.
 */
export declare type VersionNotSupportedResponse = {
    error: 'VersionNotSupported';
    versionType?: 'pull' | 'push' | 'schema' | undefined;
};

export declare type View = EntryList | Entry | undefined;

/**
 * `applyChange` does not consume the `relationships` of `ChildChange#node`,
 * `EditChange#node` and `EditChange#oldNode`.  The `ViewChange` type
 * documents and enforces this via the type system.
 */
export declare type ViewChange = AddViewChange | RemoveViewChange | ChildViewChange | EditViewChange;

export declare type ViewFactory<TSchema extends Schema, TTable extends keyof TSchema['tables'] & string, TReturn, T> = (query: Query<TSchema, TTable, TReturn>, input: Input, format: Format, onDestroy: () => void, onTransactionCommit: (cb: () => void) => void, queryComplete: true | Promise<true>, updateTTL: (ttl: TTL) => void) => T;

export declare class Zero<const S extends Schema, MD extends CustomMutatorDefs | undefined = undefined> {
    #private;
    readonly version: string;
    readonly userID: string;
    readonly storageKey: string;
    readonly queryDelegate: QueryDelegate;
    readonly query: MakeEntityQueriesFromSchema<S>;
    /**
     * Constructs a new Zero client.
     */
    constructor(options: ZeroOptions<S, MD>);
    preload(query: Query<S, keyof S['tables'] & string, any>, options?: PreloadOptions | undefined): {
        cleanup: () => void;
        complete: Promise<void>;
    };
    /**
     * The server URL that this Zero instance is configured with.
     */
    get server(): HTTPString | null;
    /**
     * The name of the IndexedDB database in which the data of this
     * instance of Zero is stored.
     */
    get idbName(): string;
    /**
     * The schema version of the data understood by this application.
     * See [[ZeroOptions.schemaVersion]].
     */
    get schemaVersion(): string;
    /**
     * The client ID for this instance of Zero. Each instance
     * gets a unique client ID.
     */
    get clientID(): ClientID;
    get clientGroupID(): Promise<ClientGroupID>;
    /**
     * Provides simple "CRUD" mutations for the tables in the schema.
     *
     * Each table has `create`, `set`, `update`, and `delete` methods.
     *
     * ```ts
     * await zero.mutate.issue.create({id: '1', title: 'First issue', priority: 'high'});
     * await zero.mutate.comment.create({id: '1', text: 'First comment', issueID: '1'});
     * ```
     *
     * The `update` methods support partials. Unspecified or `undefined` fields
     * are left unchanged:
     *
     * ```ts
     * // Priority left unchanged.
     * await zero.mutate.issue.update({id: '1', title: 'Updated title'});
     * ```
     */
    readonly mutate: MD extends CustomMutatorDefs ? DeepMerge<DBMutator<S>, MakeCustomMutatorInterfaces<S, MD>> : DBMutator<S>;
    /**
     * Provides a way to batch multiple CRUD mutations together:
     *
     * ```ts
     * await zero.mutateBatch(m => {
     *   await m.issue.create({id: '1', title: 'First issue'});
     *   await m.comment.create({id: '1', text: 'First comment', issueID: '1'});
     * });
     * ```
     *
     * Batch sends all mutations in a single transaction. If one fails, all are
     * rolled back together. Batch can also be more efficient than making many
     * individual mutations.
     *
     * `mutateBatch` is not allowed inside another `mutateBatch` call. Doing so
     * will throw an error.
     */
    readonly mutateBatch: BatchMutator<S>;
    /**
     * Whether this Zero instance has been closed.
     *
     * Once a Zero instance has been closed it no longer syncs, you can no
     * longer query or mutate data with it, and its query views stop updating.
     */
    get closed(): boolean;
    /**
     * Closes this Zero instance.
     *
     * Once a Zero instance has been closed it no longer syncs, you can no
     * longer query or mutate data with it, and its query views stop updating.
     */
    close(): Promise<void>;
    /**
     * A rough heuristic for whether the client is currently online and
     * authenticated.
     */
    get online(): boolean;
    /**
     * Subscribe to online status changes.
     *
     * This is useful when you want to update state based on the online status.
     *
     * @param listener - The listener to subscribe to.
     * @returns A function to unsubscribe the listener.
     */
    onOnline: (listener: (online: boolean) => void) => (() => void);
    /**
     * `inspect` returns an object that can be used to inspect the state of the
     * queries a Zero instance uses. It is intended for debugging purposes.
     */
    inspect(): Promise<Inspector>;
}

/**
 * Configuration for {@linkcode Zero}.
 */
export declare interface ZeroOptions<S extends Schema, MD extends CustomMutatorDefs | undefined = undefined> {
    /**
     * URL to the zero-cache. This can be a simple hostname, e.g.
     * - "https://myapp-myteam.zero.ms"
     * or a prefix with a single path component, e.g.
     * - "https://myapp-myteam.zero.ms/zero"
     * - "https://myapp-myteam.zero.ms/db"
     *
     * The latter is useful for configuring routing rules (e.g. "/zero/\*") when
     * the zero-cache is hosted on the same domain as the application. **Note that
     * only a single path segment is allowed (e.g. it cannot be "/proxy/zero/\*")**.
     */
    server?: string | null | undefined;
    /**
     * A JWT to identify and authenticate the user. Can be provided as either:
     * - A string containing the JWT token
     * - A function that returns a JWT token
     * - `undefined` if there is no logged in user
     *
     * Token validation behavior:
     * 1. **For function providers:**
     *    When zero-cache reports that a token is invalid (expired, malformed,
     *    or has an invalid signature), Zero will call the function again with
     *    `error='invalid-token'` to obtain a new token.
     *
     * 2. **For string tokens:**
     *    Zero will continue to use the provided token even if zero-cache initially
     *    reports it as invalid. This is because zero-cache may be able to validate
     *    the token after fetching new public keys from its configured JWKS URL
     *    (if `ZERO_AUTH_JWKS_URL` is set).
     */
    auth?: string | ((error?: 'invalid-token') => MaybePromise<string | undefined>) | undefined;
    /**
     * A unique identifier for the user. Must be non-empty.
     *
     * Each userID gets its own client-side storage so that the app can switch
     * between users without losing state.
     *
     * This must match the `sub` claim of the `auth` token if
     * `auth` is provided.
     */
    userID: string;
    /**
     * Distinguishes the storage used by this Zero instance from that of other
     * instances with the same userID. Useful in the case where the app wants to
     * have multiple Zero instances for the same user for different parts of the
     * app.
     */
    storageKey?: string | undefined;
    /**
     * Determines the level of detail at which Zero logs messages about
     * its operation. Messages are logged to the `console`.
     *
     * When this is set to `'debug'`, `'info'` and `'error'` messages are also
     * logged. When set to `'info'`, `'info'` and `'error'` but not
     * `'debug'` messages are logged. When set to `'error'` only `'error'`
     * messages are logged.
     *
     * Default is `'error'`.
     */
    logLevel?: LogLevel | undefined;
    /**
     * This defines the schema of the tables used in Zero and their relationships
     * to one another.
     */
    schema: S;
    /**
     * `mutators` is a map of custom mutator definitions. The keys are
     * namespaces or names of the mutators. The values are the mutator
     * implementations. Client side mutators must be idempotent as a
     * mutation can be rebased multiple times when folding in authoritative
     * changes from the server to the client.
     */
    mutators?: MD | undefined;
    /**
     * Custom mutations are pushed to zero-cache and then to
     * your API server.
     *
     * push.queryParams can be used to augment the URL
     * used to connect to your API server so it includes
     * variables in the query string.
     *
     * DEPRECATED: Use `userMutateParams` instead.
     */
    push?: UserMutateParams;
    mutate?: UserMutateParams;
    query?: UserQueryParams;
    /**
     * `onOnlineChange` is called when the Zero instance's online status changes.
     *
     * @deprecated Use `onOnline` on the Zero instance instead. e.g.
     * ```ts
     * const zero = new Zero({...});
     * zero.onOnline((online) => { ... });
     * ```
     */
    onOnlineChange?: ((online: boolean) => void) | undefined;
    /**
     * `onUpdateNeeded` is called when a client code update is needed.
     *
     * See {@link UpdateNeededReason} for why updates can be needed.
     *
     * The default behavior is to reload the page (using `location.reload()`).
     * Provide your own function to prevent the page from
     * reloading automatically. You may want to display a toast to inform the end
     * user there is a new version of your app available and prompt them to
     * refresh.
     */
    onUpdateNeeded?: ((reason: UpdateNeededReason) => void) | undefined;
    /**
     * `onClientStateNotFound` is called when this client is no longer able
     * to sync with the zero-cache due to missing synchronization state.  This
     * can be because:
     * - the local persistent synchronization state has been garbage collected.
     *   This can happen if the client has no pending mutations and has not been
     *   used for a while (e.g. the client's tab has been hidden for a long time).
     * - the zero-cache fails to find the server side synchronization state for
     *   this client.
     *
     * The default behavior is to reload the page (using `location.reload()`).
     * Provide your own function to prevent the page from reloading automatically.
     */
    onClientStateNotFound?: (() => void) | undefined;
    /**
     * The number of milliseconds to wait before disconnecting a Zero
     * instance whose tab has become hidden.
     *
     * Instances in hidden tabs are disconnected to save resources.
     *
     * Default is 5_000.
     */
    hiddenTabDisconnectDelay?: number | undefined;
    /**
     * This gets called when the Zero instance encounters an error. The default
     * behavior is to log the error to the console. Provide your own function to
     * prevent the default behavior.
     */
    onError?: OnError | undefined;
    /**
     * Determines what kind of storage implementation to use on the client.
     *
     * Defaults to `'idb'` which means that Zero uses an IndexedDB storage
     * implementation. This allows the data to be persisted on the client and
     * enables faster syncs between application restarts.
     *
     * By setting this to `'mem'`, Zero uses an in memory storage and
     * the data is not persisted on the client.
     *
     * You can also set this to a function that is used to create new KV stores,
     * allowing a custom implementation of the underlying storage layer.
     */
    kvStore?: 'mem' | 'idb' | StoreProvider | undefined;
    /**
     * The maximum number of bytes to allow in a single header.
     *
     * Zero adds some extra information to headers on initialization if possible.
     * This speeds up data synchronization. This number should be kept less than
     * or equal to the maximum header size allowed by the zero-cache and any load
     * balancers.
     *
     * Default value: 8kb.
     */
    maxHeaderLength?: number | undefined;
    /**
     * The maximum amount of milliseconds to wait for a materialization to
     * complete (including network/server time) before printing a warning to the
     * console.
     *
     * Default value: 5_000.
     */
    slowMaterializeThreshold?: number | undefined;
    /**
     * UI rendering libraries will often provide a utility for batching multiple
     * state updates into a single render. Some examples are React's
     * `unstable_batchedUpdates`, and solid-js's `batch`.
     *
     * This option enables integrating these batch utilities with Zero.
     *
     * When `batchViewUpdates` is provided, Zero will call it whenever
     * it updates query view state with an `applyViewUpdates` function
     * that performs the actual state updates.
     *
     * Zero updates query view state when:
     * 1. creating a new view
     * 2. updating all existing queries' views to a new consistent state
     *
     * When creating a new view, that single view's creation will be wrapped
     * in a `batchViewUpdates` call.
     *
     * When updating existing queries, all queries will be updated in a single
     * `batchViewUpdates` call, so that the transition to the new consistent
     * state can be done in a single render.
     *
     * Implementations must always call `applyViewUpdates` synchronously.
     */
    batchViewUpdates?: ((applyViewUpdates: () => void) => void) | undefined;
    /**
     * The maximum number of recent queries, no longer subscribed to by a preload
     * or view, to continue syncing.
     *
     * Defaults is 0.
     *
     * @deprecated Use ttl instead
     */
    maxRecentQueries?: number | undefined;
    /**
     * Changes to queries are sent to server in batches. This option controls
     * the number of milliseconds to wait before sending the next batch.
     *
     * Defaults is 10.
     */
    queryChangeThrottleMs?: number | undefined;
}

export { }
