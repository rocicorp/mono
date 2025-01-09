# Zero

## Documentation

[https://zero.rocicorp.dev/docs/introduction](https://zero.rocicorp.dev/docs/introduction)

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
npm install
npm run build
cd packages/zero
npm pack
```

This creates a tgz (tarball) file in the `packages/zero` directory. You can then install this package in another project by running:

```bash
npm install /path/to/rocicorp-zero-<VERSION>.tgz
```
