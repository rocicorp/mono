# Project Guidelines for Claude

## Code Quality
1. **Always run lint and format after every change** - This ensures code consistency and catches potential issues early.
2. **Always run TS checks after every change** - Most packages have a `type-check` command that must pass before committing changes.

## TypeScript Conventions
1. **Optional fields convention**: In this codebase, whenever there's an optional field (marked with `?`), the type is always explicitly defined as `type | undefined`. 
   - Example: `foo?: number | undefined` (not just `foo?: number`)
   - You can always explicitly pass `undefined` for optional fields in this codebase
