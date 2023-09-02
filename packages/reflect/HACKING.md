NOTE: This file is HACKING.md, not README.md, because npmjs.com shows the
README automatically on the package homepage.

# Cutting a release

## Decide what the new version should be.

Reflect is still on [major version 0](https://semver.org/#spec-item-4), so
normal semantic versioning does not apply.

Generally we bump the minor version for new release, and bump the patch version
if we are just making minor build fixes for the minor version.

## Pull main

You probably want to ensure you are at tip before you do all of the below work so that the release has latest goodness on it.

## Build the release

```
git branch -D release
git checkout -b release
cd packages/reflect
npm version minor # or patch
cd ../..
npx syncpack fix-mismatches
npm install
npm run build
cd -
npm pack --foreground-script
git commit -a -m 'Bump reflect version to $version'
```

## Manual Testing

To test that a release works before creating the release we use a tarball dependency.

### Template App

Make sure the `create` command still works as well as the app it generates.

```bash
npx /path/to/rocicorp-reflect-<version>.tgz create my-app
cd my-app
npm install

# Should run against local server
npx reflect dev

VITE_WORKER_URL="ws://127.0.0.1:8080/" npm run dev

# Should ask where to publish on vercel and run on vercel
# Need to set VITE_WORKER_URL env var on deployment
npx reflect publish
npx vercel
```

Note: The created app here will use the previous public version of Reflect, not the
new one just built above. That is fine. What we're testing here is that `create` works.
We will test the new build of Reflect below.

If you want to test the new build works with the counter app, you can also do that by
manually installing the new build:

```bash
cd my-app
npm add /path/to/rocicorp-reflect-<version>.tgz
npm run dev
```

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

## Tag the Release

```
# Make sure you're at the commit that bumps the version. We
# want to tag the exact code you just tested, not something
# potentially merged with other parallel changes on `main`.
git tag "reflect/v$NEW_VERSION"
git push --tags
```

## Merge the Release

```
# pull latest upstream
git checkout main
git pull

git branch -D release
git checkout -b release origin/main

# This will typically be a fast-forward, but if other changes
# have happened on main it will be a true merge. If there are
# merge conflicts
git merge "reflect/v$NEW_VERSION"

git push origin release
```

Then send the code review and land as normal.

## Update the peer libraries for compat with the new Reflect

The following have peerDependencies that should to be updated to the new Reflect version:

- none for now

## Publish the Release

```
git checkout reflect/v$NEW_VERSION
cd packages/reflect

# note: this will publish the release to the "latest" tag, which means it's what
# people will get when they `npm install`. If this is a beta release, you should
# add the `--tag=beta` flag to this command but also make sure the semver has
# beta in it.
npm publish
```

## Upload the Server to Mirror

This is needed so that we can publish apps to Mirror that use this version.

```bash
cd $REPO_ROOT
git checkout reflect/v$NEW_VERSION

# Change packages/reflect/package.json version to the next number
# temporarily. So say that the version is currently 0.40.5, change it to
# 0.40.6. This is a temporary workaround for:
# https://github.com/rocicorp/mono/issues/833.

npm install
cd mirror/mirror-cli
# adjust channels to taste
# can also pass --force to overwrite old versions
npm run start uploadServer -- --channels=canary --channels=stable
npm run start uploadServer -- --stack=staging --channels=canary --channels=stable

# Abandon temporary change to package.json
git reset --hard HEAD
```

## Release a Server to more channels

If a server was only uploaded to, say `--channels=canary` and you wish to roll it
out to `stable`:

```bash
cd mirror/mirror-cli
npm run start releaseServer -- --server=0.40.5 --channels=stable
```

The command is additive and will not remove any existing channels (e.g. "canary").
This can also be used for one-off debugging, e.g. if you want to release to a specific
app. Define your temporary channel name (e.g. "debug-#433") and release the desired server to
that channel. Then in the [Firebase console](https://console.firebase.google.com/project/reflect-mirror-prod/firestore/data/~2Fapps), navigate to the `apps/${appID}` document and set that App's
`serverReleaseChannel` to your channel name. The deployment should automatically kick in.
Don't forget to return the App to its original channel (and clean up the server doc).

## Unrelease a Server from a channel

```bash
cd mirror/mirror-cli
npm run start unreleaseServer -- --server=0.40.5 --channels=stable
```

Essentially, `releaseServer` and `unreleaseServer` take the same cli arguments.

## Update sample apps that depend on Reflect

- reflect-todo
- replidraw-do

## Write Release Notes

Add Release Notes to [Notion](https://www.notion.so/replicache/Release-Notes-43b93bd9bf774de6a505247a6e7a3fb8) following the pattern of
previous release notes.

Publish the page and share it to the [#reflect-alpha Discord channel](https://discord.gg/9PzrG5Qv).

---
