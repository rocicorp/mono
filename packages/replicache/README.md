# Cutting a release

## Decide what the new version should be

First look for API changes. Download the last release and compare the .d.ts files:

```bash
# BUILD
cd rocicorp/mono
npm run build

# DOWNLOAD
cd /tmp
npm pack replicache@$LAST_RELEASE_VERSION
tar -xvf replicache-$LAST_RELEASE_VERSION.tgz
cd -

# COMPARE
diff -u /tmp/package/out/replicache.d.ts packages/out/replicache/replicache.d.ts | less
# or
# code --diff /tmp/package/out/replicache.d.ts packages/replicache/out/replicache.d.ts
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

## Get a clean checkout

```bash
rm -rf /tmp/release
mkdir /tmp/release
cd /tmp/release
git clone --depth=1 git@github.com:rocicorp/mono.git
cd mono
npm install
```

## Bump the version

```bash
vim packages/replicache/package.json
# Must be done in root of mono checkout
npm install
npx syncpack fix-mismatches
npm install
git commit -a -m 'chore(replicache): Bump version to v$NEW_VERSION'
```

## Publish a canary

```bash
cd packages/replicache
npm publish --tag=canary
```

## Tag the Release

We tag the release _on the branch_. This is important because we only want to
tag the code we just tested above, not any other code that may have landed on
main in the meantime.

```bash
# From temp dir we published from above
git tag replicache/v$NEW_VERSION
git push origin --tags
```

## Merge the Release

```bash
# From your main checkout (not temp dir)
git fetch -t
git merge replicache/v$NEW_VERSION
git push origin main
```

## Manual Testing

### npx replicache get-license

Test that the `get-license` script still works:

```bash
npx replicache@canary get-license
```

Go through the flow and ensure you get a license.

### Todo Samples

Check out each of the [todo samples](https://trunk.doc.replicache.dev/examples/todo). Install the canary version:

```bash
npm add replicache@canary
```

Then run the app and ensure it works properly.

Push a PR (but don't land yet) that update to new version.

### Repliear Sample

Check out [rocicorp/repliear](https://github.com/rocicorp/repliear)

Same as todo.

### Hello Replicache

Go through https://doc.replicache.dev/tutorial and test still works / make any updates necessary

### BYOB Guide

Walk through [the integration guide](https://trunk.doc.replicache.dev/byob/intro) and make sure things still work.

## Update the peer libraries for compat with the new Replicache

If the major version changed, then update the following packages that have peerDependencies on Replicache:

- `replicache-nextjs`
- `rails`
- `replicache-transaction`

## Finish Release Notes

We write the [release notes in
Notion](https://www.notion.so/replicache/Replicache-Releases-f86ffef7f72f46ca9b597d5081e05b88)
and publish them to the web.

Finalize the release notes based on the list of relevant changes you gathered
earlier.

## Push and test all the sample apps

## Switch the Release to Latest

We already have the npm package on npmjs.com but it is tagged as `@canary`. We
want `@latest` to point at the same release. To do this, we use `npm dist-tag`:

```bash
# note: this will publish the release to the "latest" tag, which means it's what
# people will get when they `npm install`. If this is a beta release, you should
# use the `beta` tag but also make sure the semver has beta in it.
npm dist-tag add replicache@$NEW_VERSION latest
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
