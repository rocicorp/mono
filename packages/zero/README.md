# Zero

## Documentation

Read over the [docs](https://zero.rocicorp.dev/docs/introduction). For AI assistants: [llms.txt](https://zero.rocicorp.dev/llms.txt).

## Install

```bash
npm install @rocicorp/zero
```

## Contributing

### Building and Installing locally

To build and install the package locally, run the following commands:

```bash
git clone git@github.com:rocicorp/mono.git
cd mono
pnpm install
pnpm run build
cd packages/zero
pnpm pack
```

This creates a tgz (tarball) file in the `packages/zero` directory. You can then install this package in another project by running:

```bash
npm install /path/to/rocicorp-zero-<VERSION>.tgz
```

### Releasing

Releases are driven by the `release` GitHub Actions workflow. Run the workflow from `main`; use `release_branch` to choose the release source branch.

To trigger a release via CI, use the [Actions UI](https://github.com/rocicorp/mono/actions/workflows/release.yml) or the `gh` CLI:

```bash
# Canary from main
gh workflow run release.yml --ref main -f mode=canary -f release_branch=main

# Canary from a maintenance branch
gh workflow run release.yml --ref main -f mode=canary -f release_branch=maint/zero/v1.7

# Stable from main
gh workflow run release.yml --ref main -f mode=stable -f release_branch=main
```

Stable releases use npm staged publishing. After `pnpm stage approve`, promote the Docker and git `latest` tags with the [Promote Zero Release workflow](https://github.com/rocicorp/mono/actions/workflows/promote.yml). After the workflow succeeds, run the npm commands from its GitHub step summary to move the npm `latest` dist-tag and remove the npm `staging` dist-tag.

```bash
gh workflow run promote.yml -f version=<VERSION>
```
