# GitHub Actions Performance Comparison

## Overview
This document compares the performance between `ubuntu-latest` and `Ubuntu-24-x64-16-Cores` runners for the JS workflow.

## Baseline Performance (ubuntu-latest)

**Workflow Run**: [#19072816253](https://github.com/rocicorp/mono/actions/runs/19072816253) (Run on 2025-11-04)
- **Commit**: e8f6ad36d77b8f3c053c965e02ca5eaeff3365cb
- **Total Workflow Time**: ~6 minutes 33 seconds (14:55:30 - 15:02:03)

### Individual Job Times (ubuntu-latest)

| Job Name | Duration | Start | End |
|----------|----------|-------|-----|
| Sync Package Versions | 15s | 14:55:36 | 14:55:51 |
| Verify Package Dependencies | 15s | 14:55:35 | 14:55:50 |
| Prettier | 34s | 14:55:55 | 14:56:29 |
| Lint | 37s | 14:55:55 | 14:56:32 |
| Check Types | 1m 13s | 14:55:54 | 14:57:08 |
| Test (Shard 1/3) | 3m 17s | 14:57:11 | 15:00:28 |
| Test (Shard 2/3) | 4m 51s | 14:57:11 | 15:02:02 |
| Test (Shard 3/3) | 3m 49s | 14:57:11 | 15:01:00 |
| Test PG 15 | 3m 07s | 14:57:11 | 15:00:18 |
| Test PG 16 | 3m 04s | 14:57:11 | 15:00:15 |
| Test PG 17 | 3m 25s | 14:57:12 | 15:00:37 |

### Key Metrics (ubuntu-latest)
- **Gate jobs** (parallel): ~15-18 seconds
- **Build/Lint jobs** (parallel): ~34-73 seconds
- **Test jobs** (parallel): ~3-5 minutes
- **Total CI time**: ~6.5 minutes

## Performance with Ubuntu-24-x64-16-Cores

**Runner Specifications**:
- **OS**: Ubuntu 24.04
- **vCPUs**: 16 cores
- **RAM**: Higher capacity (exact specs depend on GitHub's configuration)
- **Storage**: SSD-backed

### Expected Performance Improvements

The Ubuntu-24-x64-16-Cores runner should provide:
1. **More CPU cores**: 16 cores vs standard 2-4 cores
2. **Better parallelization**: Especially for npm/bun install and test execution
3. **Faster build times**: TypeScript compilation and linting should benefit from more cores
4. **Improved test execution**: Vitest and Playwright tests can use more workers

### Runner Availability Status

> **⚠️ Important**: The workflow runs show status "action_required" which indicates that the `Ubuntu-24-x64-16-Cores` runner label may not be configured or available for this repository.
> 
> **Next Steps Required**:
> 1. Verify runner label availability with GitHub repository administrator
> 2. Ensure GitHub-hosted larger runners are enabled for the organization/repository
> 3. The runner label may need to be: `ubuntu-24.04-16core` or similar variant
> 4. Check GitHub's documentation for the exact runner label naming convention
>
> See: [About larger runners](https://docs.github.com/en/actions/using-github-hosted-runners/about-larger-runners)

### Results (Ubuntu-24-x64-16-Cores)

> **Note**: This section will be populated after the runner is properly configured and the first successful run completes.

**Workflow Run**: _Waiting for runner configuration_

| Job Name | Duration | Improvement |
|----------|----------|-------------|
| Sync Package Versions | TBD | TBD |
| Verify Package Dependencies | TBD | TBD |
| Prettier | TBD | TBD |
| Lint | TBD | TBD |
| Check Types | TBD | TBD |
| Test (Shard 1/3) | TBD | TBD |
| Test (Shard 2/3) | TBD | TBD |
| Test (Shard 3/3) | TBD | TBD |
| Test PG 15 | TBD | TBD |
| Test PG 16 | TBD | TBD |
| Test PG 17 | TBD | TBD |

### Analysis

_To be completed after the first run._

Factors to consider:
- **CPU-bound tasks**: TypeScript compilation, linting, test execution
- **I/O-bound tasks**: npm/bun install, git operations
- **Parallelization**: How well do tests scale with more cores?
- **Cost-benefit**: Is the performance improvement worth the increased cost?

## Recommendations

_To be completed after analysis._

## How to Update This Document

After the first successful run on Ubuntu-24-x64-16-Cores:

1. Get the workflow run ID and job details
2. Calculate actual timings for each job
3. Compare with baseline metrics
4. Calculate percentage improvements
5. Add analysis and recommendations
