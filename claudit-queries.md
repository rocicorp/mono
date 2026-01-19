# Security Review: Custom Queries

**Reviewed Packages**: `packages/zero-cache/`, `packages/zql/`, `packages/zero-server/`
**Focus Area**: Custom query functionality
**Date**: 2026-01-19

---

## Executive Summary

The custom query implementation demonstrates **strong security practices** overall, with multiple layers of defense. I identified a few areas that warrant attention, though no critical vulnerabilities were found.

---

## Security Controls (Working Well)

### 1. SQL Injection Prevention

**Location**: `packages/z2s/src/sql.ts`, `packages/z2s/src/compiler.ts`

- **Parameterized queries**: All user values flow through `sql.value()` which creates parameterized placeholders (`$1`, `$2`, etc.)
- **Identifier escaping**: Uses `@databases/escape-identifier` library for table/column names
- **Operator validation**: While `sql.__dangerous__rawValue()` is used for operators (`compiler.ts:394`), the operators are strictly validated against a closed set via Valita schema:

```typescript
// packages/zero-protocol/src/ast.ts:50-55
export const simpleOperatorSchema = v.union(
  equalityOpsSchema,  // '=', '!=', 'IS', 'IS NOT'
  orderOpsSchema,     // '<', '>', '<=', '>='
  likeOpsSchema,      // 'LIKE', 'NOT LIKE', 'ILIKE', 'NOT ILIKE'
  inOpsSchema,        // 'IN', 'NOT IN'
);
```

### 2. Table/Column Name Validation

**Location**: `packages/zero-types/src/name-mapper.ts`

The `NameMapper` class validates all table and column references against the schema before query execution:

```typescript
// name-mapper.ts:22-30
#getTable(src: string, ctx?: JSONValue): DestNames {
  const table = this.#tables.get(src);
  if (!table) {
    throw new Error(`unknown table "${src}"`);
  }
  return table;
}
```

Unknown tables/columns cause immediate errors, preventing arbitrary table access.

### 3. URL Allowlisting

**Location**: `packages/zero-cache/src/custom/fetch.ts:78-96`

Custom query URLs are validated against configured patterns before any request:

```typescript
if (!urlMatch(url, allowedUrlPatterns)) {
  throw new ProtocolErrorWithLevel({
    message: `URL "${url}" is not allowed by the ZERO_QUERY_URL configuration`,
    ...
  });
}
```

### 4. Read Authorization

**Location**: `packages/zero-cache/src/auth/read-authorizer.ts`

- Permission rules are applied to query ASTs before execution
- **Oracle attack prevention**: Permissions are recursively applied to subqueries in conditions (lines 127-152)
- Default deny: No rows returned if no permission rules exist for a table

### 5. Response Validation

**Location**: `packages/zero-cache/src/custom/fetch.ts:180`

Responses from external API servers are validated against Valita schemas:

```typescript
return validator.parse(json);
```

---

## Potential Security Concerns

### 1. LIKE Pattern Escaping is Manual

**Severity**: Low
**Location**: `packages/zql/src/query/escape-like.ts`

The `escapeLike()` function is exported for client use but requires manual invocation:

```typescript
export function escapeLike(val: string) {
  return val.replace(/[%_]/g, '\\$&');
}
```

**Risk**: If developers pass user input directly to LIKE queries without escaping, wildcards (`%`, `_`) could allow broader matching than intended. This is not SQL injection but could be an authorization bypass.

**Recommendation**: Consider auto-escaping LIKE pattern special characters unless the developer explicitly opts into wildcards.

### 2. Custom Query API Server Trust Model

**Severity**: Medium (by design)
**Location**: `packages/zero-cache/src/custom-queries/transform-query.ts`

The external API server (configured via `ZERO_QUERY_URL`) is trusted to return valid AST structures. While the response is schema-validated, the API server can:
- Return queries for any table defined in the schema
- Construct any valid condition structure

**Risk**: A compromised or malicious API server could return queries that access data the user shouldn't see. The authorization layer mitigates this, but it relies on correctly configured permission rules.

**Recommendation**: Document this trust model clearly. Consider adding optional table allowlisting per custom query name.

### 3. Cache Key Contains Raw Token

**Severity**: Low
**Location**: `packages/zero-cache/src/custom-queries/transform-query.ts:174-178`

```typescript
function getCacheKey(headerOptions: HeaderOptions, queryID: string) {
  return `${headerOptions.token}:${headerOptions.cookie}:${queryID}`;
}
```

**Risk**: If cache is exposed via debug endpoints or logs, raw tokens could leak.

**Recommendation**: Hash the token before using it in cache keys.

### 4. Error Messages May Leak Schema Information

**Severity**: Informational
**Location**: Various files

Error messages include table and column names:
```typescript
throw new Error(`unknown table "${src}"`);
throw new Error(`unknown column "${src}" of "${table}" table`);
```

**Risk**: Attackers could enumerate valid table/column names through error messages.

**Recommendation**: Consider using generic error messages in production while preserving detailed messages for logging.

---

## Security Architecture

```
Client Request (custom query name + args)
       │
       ▼
┌──────────────────────────────────────┐
│ 1. URL Allowlist Check               │ ← Block unauthorized URLs
└──────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│ 2. External API Server Transform     │ ← Returns AST (trusted)
└──────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│ 3. AST Schema Validation (Valita)    │ ← Validates structure
└──────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│ 4. Authorization Transform           │ ← Adds permission rules
└──────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│ 5. Name Mapper Validation            │ ← Validates table/column names
└──────────────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│ 6. SQL Compilation (Parameterized)   │ ← Escapes identifiers, parameterizes values
└──────────────────────────────────────┘
       │
       ▼
    Database
```

---

## Key Files Reviewed

| File | Purpose | Security Role |
|------|---------|---------------|
| `zero-cache/src/custom-queries/transform-query.ts` | Query transformation orchestration | Request routing, caching, error handling |
| `zero-cache/src/custom/fetch.ts` | API server communication | URL allowlisting, header construction, validation |
| `zero-cache/src/auth/read-authorizer.ts` | Permission application | Authorization enforcement |
| `zero-server/src/custom.ts` | Server transaction + CRUD | SQL generation, parameterization |
| `z2s/src/sql.ts` | SQL building | Parameter binding, identifier escaping |
| `z2s/src/compiler.ts` | AST to SQL compilation | Query transformation, type handling |
| `zero-types/src/name-mapper.ts` | Name validation | Table/column allowlisting |
| `zero-protocol/src/ast.ts` | AST schema | Operator validation, structure validation |
| `zero-protocol/src/custom-queries.ts` | Protocol definitions | Request/response schemas |

---

## Findings Summary

| Finding | Severity | Status |
|---------|----------|--------|
| SQL injection via operators | Critical | **Mitigated** by schema validation |
| SQL injection via values | Critical | **Mitigated** by parameterized queries |
| Arbitrary table access | High | **Mitigated** by NameMapper validation |
| SSRF via custom query URL | High | **Mitigated** by URL allowlisting |
| Authorization bypass via subqueries | High | **Mitigated** by recursive permission application |
| LIKE wildcard abuse | Low | **Manual mitigation** via `escapeLike()` |
| API server trust model | Medium | **By design** - requires correct permission config |
| Cache key token exposure | Low | Consider hashing tokens in cache keys |
| Schema enumeration via errors | Informational | Consider generic production errors |

---

## Conclusion

The codebase demonstrates security-conscious design with defense-in-depth. No critical or high-severity vulnerabilities were identified. The potential concerns noted are either low severity or represent documented trust boundaries that are mitigated by proper configuration of permission rules.