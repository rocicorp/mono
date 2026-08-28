/**
 * @name Customer application data written to logs
 * @description Finds Zero customer data that flows into logs or thrown error messages.
 * @kind path-problem
 * @problem.severity error
 * @precision medium
 * @id js/zero-customer-data-in-log
 * @tags security
 *       external/cwe/cwe-532
 */

import javascript

/** Names of types whose values can contain customer application data. */
private predicate isSensitiveZeroTypeName(string name) {
  name = [
      "AST",
      "Condition",
      "LiteralValue",
      "LiteRow",
      "LiteRowKey",
      "Row",
      "RowList",
      "RowValue",
      "InsertOp",
      "UpdateOp",
      "DeleteOp",
      "CRUDOp",
      "CRUDMutationArg",
      "CustomMutation",
      "Mutation",
      "Auth",
      "JWTAuth"
    ]
}

/** A file in this repository, as opposed to one in a dependency. */
private predicate isRepositoryFile(File file) {
  file.getRelativePath().regexpMatch("^(packages|apps|tools)/.*")
}

/**
 * A type annotation naming one of the types above, resolving to a declaration
 * in this repository.
 *
 * Matching is on the annotation rather than through `hasUnderlyingType`,
 * because CodeQL records a module name only for bare package specifiers: the
 * Zero types are imported by relative path, so no module-based match can ever
 * see one. Requiring the declaration to live in the repository keeps
 * same-named types from third-party packages out.
 *
 * `getAnUnderlyingType` unwraps unions, aliases, optionals and arrays, so
 * `Row | undefined` and `RowValue[]` match as well.
 */
private predicate isSensitiveTypeAnnotation(TypeAnnotation annotation) {
  exists(LocalTypeAccess access |
    access = annotation.getAnUnderlyingType() and
    isSensitiveZeroTypeName(access.getName()) and
    isRepositoryFile(access.getLocalTypeName().getADeclaration().getFile())
  )
}

/**
 * A value declared with one of the Zero types, at every reference to it.
 *
 * Covers parameters and variable declarations, which is where these types are
 * introduced. A value whose type is only inferred -- a call result that was
 * never annotated -- is not matched.
 */
private predicate hasSensitiveZeroType(DataFlow::Node node) {
  exists(BindingPattern pattern |
    isSensitiveTypeAnnotation(pattern.getTypeAnnotation()) and
    node.asExpr() = pattern.getAVariable().getAnAccess()
  )
}

/** A value whose type is known to contain customer data. */
private predicate hasSensitiveType(DataFlow::Node node) {
  hasSensitiveZeroType(node)
  or
  node.hasUnderlyingType("jose", "JWTPayload")
  or
  node.hasUnderlyingType("postgres", "Notice")
  or
  node.hasUnderlyingType("postgres", "PostgresError")
}

/** Properties of sensitive containers that contain names rather than values. */
private predicate isSafeMetadataProperty(string name) {
  name = [
      "column",
      "columnName",
      "columns",
      "columnSpec",
      "clientSchema",
      "code",
      "constraint_name",
      "constructor",
      "op",
      "primaryKey",
      "routine",
      "schema",
      "schema_name",
      "severity",
      "string",
      "table",
      "tableName",
      "table_name",
      "type"
    ]
}

/**
 * A read from a sensitive object is itself a source. Making the read a source
 * keeps the analysis useful after a value has been destructured to a primitive
 * type such as `string`.
 */
private predicate isSensitivePropertyRead(DataFlow::Node node) {
  exists(DataFlow::PropRead read, DataFlow::Node base, string propertyName |
    node = read and
    read.accesses(base, propertyName) and
    hasSensitiveType(base) and
    not isSafeMetadataProperty(propertyName)
  )
  or
  // SQL bind parameters are commonly typed as object[] rather than a named
  // Zero type, so seed taint at the property read.
  exists(DataFlow::PropRead read |
    node = read and read.getPropertyName() = "parameters"
  )
}

private predicate isLogMethod(string name) {
  name = ["debug", "error", "info", "log", "warn"]
}

private predicate isLogContextCall(DataFlow::MethodCallNode call) {
  call.getReceiver().hasUnderlyingType("@rocicorp/logger", "LogContext") and
  (isLogMethod(call.getMethodName()) or call.getMethodName() = "withContext")
}

private predicate isLogSinkCall(DataFlow::MethodCallNode call) {
  call.getReceiver().hasUnderlyingType("@rocicorp/logger", "LogSink") and
  call.getMethodName() = "log"
}

private predicate isConsoleCall(DataFlow::MethodCallNode call) {
  call.getReceiver().accessesGlobal("console") and isLogMethod(call.getMethodName())
}

/**
 * Code that ships, as opposed to tests, benchmarks, and developer tools.
 *
 * CLI entry points and the throughput harness print queries and ASTs as their
 * whole purpose, so a leak there is not a leak.
 */
private predicate isProductionNode(DataFlow::Node node) {
  exists(string path | path = node.getFile().getRelativePath() |
    not path.matches("%.test.%") and
    not path.matches("%.bench.%") and
    not path.matches("%/test/%") and
    not path.matches("%/bin.ts") and
    not path.matches("%/bin-%.ts") and
    not path.matches("apps/zero-throughput/%")
  )
}

/**
 * Generic helpers that throw whatever they are handed.
 *
 * Reporting the `throw` inside one collapses every caller onto a single line:
 * `assert` alone accounted for 128 paths. They are modelled as sinks at the
 * call site instead, by `isThrowingHelperCall`, so an alert names the code
 * that supplied the value.
 */
private predicate isGenericThrowHelperFile(File file) {
  file.getRelativePath() =
    ["packages/shared/src/asserts.ts", "packages/shared/src/valita.ts"]
}

/**
 * An argument that one of those helpers puts into the message it throws.
 *
 * Which argument matters. `assert(cond, msg)` throws only its message, so its
 * condition -- usually an AST-derived boolean -- discloses nothing. The
 * `assertX` family routes through `invalidType`, which interpolates the value
 * itself, so there the value is the sink. `unreachable`, `notImplemented`,
 * and `assertNotNull` throw fixed strings and are not sinks at all.
 */
private predicate isThrowingHelperArgument(DataFlow::Node node) {
  exists(DataFlow::InvokeNode call |
    (
      call.getCalleeName() = "assert" and
      node = call.getArgument(1)
    )
    or
    (
      call.getCalleeName() =
        [
          "assertArray", "assertBoolean", "assertNumber", "assertObject",
          "assertString", "assertType", "throwInvalidType"
        ] and
      node = call.getArgument(0)
    )
  )
}

/**
 * A caught exception.
 *
 * Taint stops at a `catch`. Where an error was built out of a value, the
 * error-construction sink already reports the place it was built; letting the
 * taint continue would report every `catch (e) { lc.warn(..., e) }` in the
 * tree a second time, which is where the largest path counts came from.
 */
private predicate isCaughtError(DataFlow::Node node) {
  exists(CatchClause clause |
    node.asExpr() = clause.getAParameter().getAVariable().getAnAccess()
  )
}

/** Calls whose return value is an explicitly approved safe representation. */
private predicate isApprovedSanitizer(DataFlow::Node node) {
  // Replace these syntactic checks with API::moduleImport(...) once the safe
  // helper has a permanent module and export path.
  exists(DataFlow::InvokeNode call |
    node = call and call.getCalleeName() = "safe"
  )
  or
  exists(DataFlow::MethodCallNode call |
    node = call and
    call.getMethodName() = ["count", "hash", "shape"] and
    call.getReceiver().asExpr().(VarRef).getName() = "safe"
  )
}

/**
 * A count read off a container.
 *
 * `xs.length` and `set.size` are numbers derived from a value rather than the
 * value itself, so they cannot carry row data -- the same reasoning that makes
 * `safe.count()` a sanitizer. Without this, logging the number of bound
 * parameters in a statement reads as logging the parameters.
 */
private predicate isCountRead(DataFlow::Node node) {
  exists(DataFlow::PropRead read |
    node = read and read.getPropertyName() = ["length", "size"]
  )
}

/**
 * A read through a unique symbol key.
 *
 * Rows arrive from SQLite and JSON with string keys only, so nothing outside
 * the module holding the symbol can put a value at one: what comes back is
 * internal metadata such as a refcount. `assertNumber` and friends do
 * interpolate what they are handed, but through a symbol key they can only
 * ever see that metadata.
 *
 * Resolution is local, so this covers a symbol used in the file that declares
 * it, which is how they are used here.
 */
private predicate isSymbolKeyedRead(DataFlow::Node node) {
  exists(DataFlow::PropRead read, DataFlow::CallNode symbol |
    node = read and
    symbol = read.getPropertyNameExpr().flow().getALocalSource() and
    symbol.getCalleeName() = "Symbol"
  )
}

/**
 * A safe field selected off any value.
 *
 * The base is deliberately unconstrained. Requiring it to have a sensitive
 * type would switch the exemption off the moment a value has been through a
 * loop, a destructuring, or a call, since it is then merely tainted rather
 * than annotated -- which is how `op.tableName` came to be reported despite
 * `tableName` being on the safe list.
 */
private predicate isSafePropertyRead(DataFlow::Node node) {
  exists(DataFlow::PropRead read |
    node = read and isSafeMetadataProperty(read.getPropertyName())
  )
}

/** String and error wrappers that should preserve taint. */
private predicate isWrapperCall(DataFlow::InvokeNode call) {
  call.getCalleeName() = [
      "String",
      "inspect",
      "invalidType",
      "stringify",
      "toErrorLogObject",
      "toString"
    ]
}

/**
 * Holds if `node` is marked with a `// log-leak-ignore` comment, either at the
 * end of its own line or on the line directly above it.
 *
 * Suppression is deliberately line-based rather than scope-based: it should
 * cover exactly the log call it sits on, so that a later edit adding a second
 * value to the same call does not inherit the exemption.
 */
private predicate isSuppressed(DataFlow::Node node) {
  exists(Comment comment, Location commentLocation, Location nodeLocation |
    comment.getText().matches("%log-leak-ignore%") and
    commentLocation = comment.getLocation() and
    nodeLocation = node.getLocation() and
    commentLocation.getFile() = nodeLocation.getFile() and
    nodeLocation.getStartLine() =
      [commentLocation.getStartLine(), commentLocation.getStartLine() + 1]
  )
}

module LogLeakConfig implements DataFlow::ConfigSig {
  predicate isSource(DataFlow::Node source) {
    hasSensitiveType(source) or isSensitivePropertyRead(source)
  }

  predicate isSink(DataFlow::Node sink) {
    isProductionNode(sink) and
    not isSuppressed(sink) and
    not isGenericThrowHelperFile(sink.getFile()) and
    (
      exists(DataFlow::MethodCallNode call |
        sink = call.getAnArgument() and
        (isLogContextCall(call) or isLogSinkCall(call) or isConsoleCall(call))
      )
      or
      // Error construction is a sink because the Error may be logged later.
      exists(DataFlow::NewNode call |
        sink = call.getAnArgument() and
        call.getCalleeName() = [
            "AggregateError",
            "Error",
            "EvalError",
            "RangeError",
            "ReferenceError",
            "SyntaxError",
            "TypeError",
            "URIError"
          ]
      )
      or
      isThrowingHelperArgument(sink)
    )
  }

  predicate isBarrier(DataFlow::Node node) {
    isApprovedSanitizer(node) or
    isSafePropertyRead(node) or
    isCountRead(node) or
    isCaughtError(node) or
    isSymbolKeyedRead(node)
  }

  predicate isAdditionalFlowStep(DataFlow::Node predecessor, DataFlow::Node successor) {
    exists(DataFlow::InvokeNode call |
      isWrapperCall(call) and
      predecessor = call.getAnArgument() and
      successor = call
    )
    or
    exists(DataFlow::MethodCallNode call |
      call.getMethodName() = "toString" and
      predecessor = call.getReceiver() and
      successor = call
    )
  }
}

module LogLeakFlow = TaintTracking::Global<LogLeakConfig>;

import LogLeakFlow::PathGraph

from LogLeakFlow::PathNode source, LogLeakFlow::PathNode sink
where LogLeakFlow::flowPath(source, sink)
select sink.getNode(), source, sink,
  "Customer application data from $@ flows into a log or error message.", source.getNode(),
  "this sensitive value"
