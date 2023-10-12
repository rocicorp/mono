# Cutting a release

## Build the code

First make double-check the current build has passed tests on GitHub.

Then:

```bash
git checkout main
git pull
git branch -D release
git checkout -b release HEAD
npm install
npm run build
npm run test
```

## Decide what the new version should be

First look for API changes. Download the last release and compare the .d.ts files:

```bash
cd /tmp
npm pack replicache@$LAST_RELEASE_VERSION
tar -xvf replicache@$LAST_RELEASE_VERSION
cd -
diff -u /tmp/package/out/replicache.d.ts out/packages/replicache/replicache.d.ts | less
```

We need to be very careful about public API changes as we then have to maintain them. Make sure all new API has been discussed and agreed to by the team.

Next look through all changes in the repo since the last release. To do this properly:

```bash
# List all commits on main from the commit prior to last tag (which should be
# present on main) to HEAD.
git log replicache/v$LAST_RELEASE_VERSION^..HEAD
```

Build a list of all changes that affect Replicache. This will become the release notes later.

- If there are any breaking changes, then this needs to be a new major version.
- If there are any new (non-breaking) features, then this needs to be a new minor version.
- Otherwise, if there are only non-breaking bugfixes, it's a patch.

Finally update the version:

```bash
cd packages/replicache
npm version major # or minor or patch v12.0.0-beta.0
cd -
npx syncpack fix-mismatches
# Ensure all fixed, else fix manually
npx syncpack
# make sure package-lock.json is also up to date
npm install
git commit -a -m 'chore(replicache): Bump version to v$NEW_VERSION'
```

## Manual Testing

To test that a release works before creating the release we use a tarball dependency.

```bash
cd packages/replicache
npm pack
```

### npx replicache get-license

Test that the `get-license` script still works:

```bash
mv replicache-<version>.tgz /tmp/
cd /tmp/
npx replicache-<version>.tgz get-license
```

Go through the flow and ensure you get a license.

### Todo Samples

Check out each of the [todo samples](https://trunk.doc.replicache.dev/examples/todo). Manually add the tarball:

```bash
npm add /tmp/replicache-<version>.tgz
```

Then run the app and ensure it works properly.

### Repliear Sample

Check out [rocicorp/repliear](https://github.com/rocicorp/repliear)

Same as todo.

### BYOB Guide

Walk through [the integration guide](https://trunk.doc.replicache.dev/byob/intro) and make sure things still work.

## Tag the Release

We tag the release _on the branch_. This is important because we only want to
tag the code we just tested above, not any other code that may have landed on
main in the meantime.

```
cd <mono-root>
# Make sure you're still on the release branch
git tag replicache/v$NEW_VERSION
git push origin --tags
```

## Update the peer libraries for compat with the new Replicache

If the major version changed, then update the following packages that have peerDependencies on Replicache:

- `replicache-nextjs`
- `rails`

## Publish the Release

```
# note: this will publish the release to the "latest" tag, which means it's what
# people will get when they `npm install`. If this is a beta release, you should
# add the `--tag=beta` flag to this command but also make sure the semver has
# beta in it.
npm publish
```

## Publish the Private Release

We also publish a private release with the name `@rocicorp/replicache` to
[npmjs.org](https://www.npmjs.com/package/@rocicorp/replicache) which does not
minimize the code and includes the sourcemaps. This npm package may be used by
paying customers to make it easier to debug their code.

```bash
git checkout rocicorp-replicache

# Merge new release
git merge replicache/v$NEW_VERSION

# Verify that the only diff is the name and the sourcemap
git diff replicache/v$NEW_VERSION

git push origin rocicorp-replicache

npm publish
```

## Merge release to main

```bash
git checkout main
git pull
git branch -D release
git checkout -b release main
git merge replicache/v$NEW_VERSION
git commit -a -m 'Merge replicache/$NEW_VERSION'
git push origin release
```

If there were no changes on main while the release was happening,
this will be a fast-forward. Otherwise it will be a true merge.

Send a PR for this and land as normal.

## Release docs

The docs are built from the `docs` branch so we need to rebase that to get it
to deploy a new version.

```
git checkout docs
git pull
git reset --hard replicache/v$NEW_VERSION
git push origin docs
```

**Important:** Only do this when releasing a new version, otherwise we will release early docs that don't match current released code. To cherry-pick doc improvements see: "sprucing the docs", below.

**Note:** It's likely that when you `git push origin docs` above, you'll get a conflict error. This is expected if there have been any cherry-picks onto this branch as would happen if somebody "spruced" (below). Check that all the new commits on this docs branch since the last release are present in `origin/main`. To do this, for each such commit, there should be a message `Cherry-picked from <original-hash>` in the commit message. This message is added by the "spruce" procedure. Look for each such `<original-hash>` in `origin/main`. If all such commits on `docs` are present in `origin/main` then you can force the push with `git push origin docs --force`. If there is a commit on this branch which is missing from `origin/main` then somebody edited directly on this branch and it should be investigated.

**TODO:** We should write a script `release-docs.sh` to automate the above.

## Push updates to the sample apps that update their dependency on Replicache

- replicache-todo
- repliear
- replidraw{-do}

## Write Release Notes

Our release notes are now blog posts.

TODO: Document how to do this.

---

# Sprucing the docs

The live docs at doc.replicache.dev are served from the `docs` channel so that they reflect the stable API.

However, this means that if you do cleanup docs changes that you want to show up immediately, you need to cherry-pick the changes onto the `docs` branch:

```
git checkout docs
git pull
# The '-x' appends the hash of the original commit to the cherry-pick'd commit.
# This makes it easier to find missing commits during releases.
git cherry-pick -x <hash-of-spruce-commit>
git push origin docs
```

During release, below, we reset the `docs` branch to main, dropping these cherry-picked changes. So it's important to never do work directly on `docs`.

# Performance Monitoring

We continuously track performance across a variety of benchmarks and the size of Replicache's bundle.
Results here:

- [Performance Benchmarks](https://rocicorp.github.io/mono/perf-v2/)
- [Bundle Sizes](https://rocicorp.github.io/mono/bundle-sizes)

The runner runs on an ec2 instance you can find [here](https://us-east-1.console.aws.amazon.com/ec2/v2/home?region=us-east-1#InstanceDetails:instanceId=i-0492542c9af59b8e7) or through the ec2 aws console. If you need
to set it up again you can kill the existing runner processes, rm the old `actions-runner` directory, and
then [follow the github instructions for installing a runner](https://github.com/rocicorp/replicache-internal/settings/actions/runners/new?arch=x64&os=linux). You will want to use `runsvc.sh install` and `runsvc.sh start` instead of `run.sh` so it
keeps running after you detach.

# Local Debugging with Source Maps

To debug and develop Replicache locally in a webpack-based app, you need to tweak a few things.

If you haven't already done so, make `replicache-internal` available to npm by using [npm-link](https://docs.npmjs.com/cli/v8/commands/npm-link), and create a debug build.

```bash
# in your replicache-internal dir
npm link

# creates an unminified and unmangled build so that you can see symbols in the debugger/watches
npm run build -- --debug
```

In your app you want to debug, link the replicache package, and install the [`source-map-loader`](https://github.com/webpack-contrib/source-map-loader) package:

```bash
cd <your replicache app, like repliear or replicache-todo>
npm link replicache

npm install -D source-map-loader
```

Modify the webpack config to include third-party source maps in its bundle.
For our sample apps (Next.js based), modify the next config:

```js
// next.config.js
module.exports = {
  ...
  webpack: (config) => {
    config.module.rules.push({
      test: /\.mjs$/,
      use: ["source-map-loader"],
    });
    return config;
  },
  ...
}

```
