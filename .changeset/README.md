# Changesets

Changesets version the Swift client: the `zero-swift` npm package
(`packages/zero-swift-cli`) and the Swift package (`swift/`), which share one
version. Upstream Zero's packages are released separately and are ignored here.

To release a change to either, add a changeset for `zero-swift`:

```sh
pnpm dlx @changesets/cli@3.0.3
```

Changesets isn't a workspace dependency, so the root `package.json` and
`pnpm-lock.yaml` stay as upstream has them and syncing upstream doesn't
conflict there.

On `main`, `.github/workflows/swift-release.yml` keeps a "Version Packages" pull
request open with the pending version and changelog. Merging it sets the
version in `packages/zero-swift-cli/package.json` and `swift/VERSION`, and the
workflow then publishes the npm package and tags the SwiftPM mirror after
approval in the `swift-release` environment.
