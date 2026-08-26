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

/**
 * Restricts the short type names above to modules owned by Zero. CodeQL module
 * names for relative imports include the resolved source path.
 */
private predicate isZeroSourceModule(string moduleName) {
  moduleName.regexpMatch(".*(zero-protocol|zero-cache|zql)/src/.*")
}

/** A value whose resolved TypeScript type is known to contain customer data. */
private predicate hasSensitiveType(DataFlow::Node node) {
  exists(string moduleName, string typeName |
    isZeroSourceModule(moduleName) and
    isSensitiveZeroTypeName(typeName) and
    node.hasUnderlyingType(moduleName, typeName)
  )
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

private predicate isProductionNode(DataFlow::Node node) {
  not node.getFile().getRelativePath().matches("%.test.%") and
  not node.getFile().getRelativePath().matches("%.bench.%") and
  not node.getFile().getRelativePath().matches("%/test/%")
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

/** A safe field selected from an otherwise-sensitive object. */
private predicate isSafePropertyRead(DataFlow::Node node) {
  exists(DataFlow::PropRead read, DataFlow::Node base, string propertyName |
    node = read and
    read.accesses(base, propertyName) and
    hasSensitiveType(base) and
    isSafeMetadataProperty(propertyName)
  )
}

/** String and error wrappers that should preserve taint. */
private predicate isWrapperCall(DataFlow::InvokeNode call) {
  call.getCalleeName() = [
      "String",
      "inspect",
      "stringify",
      "toErrorLogObject",
      "toString"
    ]
}

module LogLeakConfig implements DataFlow::ConfigSig {
  predicate isSource(DataFlow::Node source) {
    hasSensitiveType(source) or isSensitivePropertyRead(source)
  }

  predicate isSink(DataFlow::Node sink) {
    isProductionNode(sink) and
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
      exists(ThrowStmt statement | sink.asExpr() = statement.getExpr())
    )
  }

  predicate isBarrier(DataFlow::Node node) {
    isApprovedSanitizer(node) or isSafePropertyRead(node)
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
