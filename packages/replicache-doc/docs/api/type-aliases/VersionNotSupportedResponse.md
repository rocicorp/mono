# Type Alias: VersionNotSupportedResponse

> **VersionNotSupportedResponse**: `object`

The server endpoint may respond with a `VersionNotSupported` error if it does
not know how to handle the pullVersion, pushVersion or the
schemaVersion.

## Type declaration

### error

> **error**: `"VersionNotSupported"`

### versionType?

> `optional` **versionType**: `"pull"` \| `"push"` \| `"schema"`
