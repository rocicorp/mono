This is a dummy file in the `maint/zero/v1.9` branch to satisfy
an expectation in the `main` branch's `release.yml` file that
specifies the docker `--build-context monogo=source/go` argument.

https://github.com/rocicorp/mono/pull/6396

Although the Dockerfile in `maint/zero/v1.9` does not yet reference
the `/go` directory, the `release.yml` command is run from `main`,
and if the directory does not exist, the workflow fails.