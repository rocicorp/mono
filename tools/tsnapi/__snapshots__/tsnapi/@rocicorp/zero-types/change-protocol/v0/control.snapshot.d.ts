// #region Variables
export declare const resetRequiredSchema: v.ObjectType<{
    tag: v.Type<"reset-required">;
    message: v.Optional<string>;
    errorDetails: v.Optional<Record<string, import("./json.ts").JSONValue | undefined>>;
}, undefined>;
// #endregion
