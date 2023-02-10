# Building a release

Get the dependencies

```
npm install
```

## Run Automated Tests

```
npm run test
```

## Manual Testing

To test that a release works before creating the release we use a tarball dependency.

```
npm pack
```

### Replidraw

Check out [rocicorp/replidraw](https://github.com/rocicorp/replidraw)

Replace the replicache dependency in
[package.json](https://github.com/rocicorp/replidraw/blob/master/package.json)
with the tarball.

```
// package.json
"replicache": "file:../replicache/replicache.tar.gz",
```

Recreate the deps:

```
npm install
```

Run Replidraw with environment variables:

```
AMAZON_ACCESS_KEY_ID=... \
AMAZON_SECRET_ACCESS_KEY=... \
AMAZON_REGION=us-west-2 \
REPLIDRAW_DB_NAME=... \
REPLIDRAW_RESOURCE_ARN=... \
REPLIDRAW_SECRET_ARN=... \
npm run dev
```

The Amazon credentials can be found in `~/.aws/credentials`. See [Replidraw
Hacking.md](https://github.com/rocicorp/replidraw/blob/master/HACKING.md) for
instructions.

You might need to initialize the DB by going to `http://localhost:3000/api/init`.

Open two windows and make sure that the changes are reflected in each window.

### Todo Sample

Check out [rocicorp/replicache-todo](https://github.com/rocicorp/replicache-todo)

Replace the replicache dependency in
[package.json](https://github.com/rocicorp/replicache-todo/blob/master/package.json)
with the tarball.

```
// package.json
"replicache": "file:../replicache/replicache.tar.gz",
```

Recreate the deps:

```
npm install
```

Start Supabase:

```
supabase start
```

Run the app:

```
DATABASE_URL=... \
NEXT_PUBLIC_SUPABASE_URL=... \
NEXT_PUBLIC_SUPABASE_KEY=... \
npm run dev
```

Open two windows and make sure that the changes are reflected in each window.

### Integration Guide

Walk through [the integration guide](https://doc.replicache.dev/guide/intro) and make sure things still work.

## Build the Release

```
cd tool/bump
go build
./bump --root=../../ <semver>
cd -
npm install
git add package-lock.json
git commit --amend --no-edit
# push to github and merge
# pull merged commit
git tag v<semver>
git push origin v<semver>
# update release notes on github

# note: this will push the release to the "latest" tag, which means it's what
# people will get when they `npm install`. If this is a beta release, you should
# add the `--tag=beta` flag to this command.
npm publish
```

## Check for API Changes

We need to be very careful about public API changes as we then have to maintain them.

Check whether there are any public API changes by diffing `out/replicache.d.ts` between the previous released version and the new candidate. Make sure all new API has been discussed and agreed to by the team.

## Release docs

The docs are built from the `docs` branch so we need to rebase that to get it
to deploy a new version.

```
git checkout docs
git pull
git reset --hard <tag-of-release>
git push origin docs
```

**Important:** Only do this when releasing a new version, otherwise we will release early docs that don't match current released code. To cherry-pick doc improvements see: "sprucing the docs", below.

**Note:** It's likely that when you `git push origin docs` above, you'll get a conflict error. This is expected if there have been any cherry-picks onto this branch as would happen if somebody "spruced" (below). Check that all the new commits on this docs branch since the last release are present in `origin/main` (note that they won't have same hash - you have to check by commit description) and if they are, then you can force the push with `git push origin docs --force`. If there is a commit on this branch which is missing from `origin/main` then somebody edited directly on this branch and it should be investigated.

# Sprucing the docs

The live docs at doc.replicache.dev are served from the `docs` channel so that they reflect the stable API.

However, this means that if you do cleanup docs changes that you want to show up immediately, you need to cherry-pick the changes onto the `docs` branch:

```
git checkout docs
git pull
git cherry-pick <hash-of-spruce-commit>
git push origin docs
```

During release, below, we reset the `docs` branch to main, dropping these cherry-picked changes. So it's important to never do work directly on `docs`.

# Performance Monitoring

We continuously track performance across a variety of benchmarks and the size of Replicache's bundle.
Results here:

- [Performance Benchmarks](https://rocicorp.github.io/replicache-internal/perf-v2/)
- [Bundle Sizes](https://rocicorp.github.io/replicache-internal/bundle-sizes)

The runner runs on an ec2 instance you can find [here](https://us-east-1.console.aws.amazon.com/ec2/v2/home?region=us-east-1#InstanceDetails:instanceId=i-0492542c9af59b8e7) or through the ec2 aws console. If you need
to set it up again you can kill the existing runner processes, rm the old `actions-runner` directory, and
then [follow the github instructions for installing a runner](https://github.com/rocicorp/replicache-internal/settings/actions/runners/new?arch=x64&os=linux). You will want to use `runsvc.sh install` and `runsvc.sh start` instead of `run.sh` so it
keeps running after you detach.
