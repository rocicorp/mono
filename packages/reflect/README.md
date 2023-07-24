# Cutting a release

## Decide what the new version should be.

Reflect is still on [major version 0](https://semver.org/#spec-item-4), so
normal semantic versioning does not apply.

Generally we bump the minor version for new release, and bump the patch version
if we are just making minor build fixes for the minor version.

## Build the release

```
git branch -D release
git checkout -b release HEAD
cd packages/reflect
npm version major # or minor
npm pack --foreground-script
```

## Manual Testing

To test that a release works before creating the release we use a tarball dependency.

### Replidraw-do

Check out [rocicorp/replidraw-do](https://github.com/rocicorp/replidraw-do)

Replace the reflect dependency in
[package.json](https://github.com/rocicorp/replidraw-do/blob/main/package.json)
with the tarball.

```
npm install /path/to/rocicorp-reflect-<version>.tgz
```

Follow instructions in repo to finish setting up app, and run to make sure it works.

### Todo Sample

Check out [rocicorp/reflect-todo](https://github.com/rocicorp/reflect-todo)

Same as Replidraw-do test.

## Check for API Changes

We need to be very careful about public API changes as we then have to maintain them.

Check whether there are any public API changes by diffing `client.d.ts` and
`server.d.ts` between the previous released version and the new candidate. Make
sure all new API has been discussed and agreed to by the team.

## Land the Release

Send out the release branch as a PR like normal and land it.

## Tag the Release

```
git checkout main
git pull
# Make sure you're at the commit that bumps the version
git tag "reflect/v$NEW_VERSION"
git push --tags
```

## Update the peer libraries for compat with the new Reflect

The following have peerDependencies that should to be updated to the new Reflect version:

- none for now

## Publish the Release

```
# note: this will publish the release to the "latest" tag, which means it's what
# people will get when they `npm install`. If this is a beta release, you should
# add the `--tag=beta` flag to this command but also make sure the semver has
# beta in it.
npm publish
```

## Update sample apps that depend on Reflect

- reflect-todo
- replidraw-do

## Write Release Notes

Add Release Notes to [Notion](https://www.notion.so/replicache/Release-Notes-43b93bd9bf774de6a505247a6e7a3fb8) following the pattern of
previous release notes.

Publish the page and share it to the [#reflect-alpha Discord channel](https://discord.gg/9PzrG5Qv).

---
