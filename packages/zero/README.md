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

Releases are driven by `packages/zero/tool/release.ts` via the `release` GitHub Actions workflow.
Run `node packages/zero/tool/release.ts --help` for the full CLI reference.

To trigger a release via CI, use the [Actions UI](https://github.com/rocicorp/mono/actions/workflows/release.yml) or the `gh` CLI:

```bash
# Canary from main
gh workflow run release.yml -f mode=canary -f ref=main

# Stable from main (or a tag / commit SHA)
gh workflow run release.yml -f mode=stable -f ref=main
gh workflow run release.yml -f mode=stable -f ref=v1.2.3
gh workflow run release.yml -f mode=stable -f ref=<40-char-sha>

# Dry run — build and version-bump without publishing
gh workflow run release.yml -f mode=canary -f ref=main -f dry_run=true
```

Stable releases are staged first. After `pnpm stage approve`, run `node packages/zero/tool/make-latest.js <version>` to promote npm and Docker tags to `latest`.

> **Note**: `-f ref=...` is the build target (branch, tag, or SHA). `--ref` selects which branch
> the _workflow file itself_ is read from — only needed when running the workflow off a non-default branch.
