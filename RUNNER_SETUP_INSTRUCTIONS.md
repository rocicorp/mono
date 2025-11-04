# GitHub Actions Larger Runner Setup Instructions

## Current Status

The `js.yml` workflow has been updated to use the `Ubuntu-24-x64-16-Cores` runner, but the workflow runs are showing "action_required" status, indicating the runner is not yet available.

## Required Actions

### For Repository Administrators

To enable the larger runners for this repository, follow these steps:

1. **Verify Organization Access**
   - Larger runners require GitHub Team or Enterprise Cloud plans
   - Check if the organization has access to larger runners
   - Navigate to: Organization Settings → Actions → Runners

2. **Configure Larger Runners**
   - Go to Repository Settings → Actions → Runners
   - Click "New runner" → "New GitHub-hosted runner"
   - Or use organization-level runner groups

3. **Verify Runner Label**
   - The exact label name for Ubuntu 24.04 with 16 cores may vary
   - Common formats:
     - `ubuntu-24.04-16core`
     - `Ubuntu-24-x64-16-Cores` (as used in this PR)
     - `ubuntu-latest-16-core`
   - Consult GitHub's documentation or your organization's runner configuration

4. **Update Workflow if Needed**
   - If the runner label differs from `Ubuntu-24-x64-16-Cores`
   - Update all `runs-on:` directives in `.github/workflows/js.yml`

## Reference Documentation

- [About larger runners](https://docs.github.com/en/actions/using-github-hosted-runners/about-larger-runners)
- [Using larger runners](https://docs.github.com/en/actions/using-github-hosted-runners/using-larger-runners)
- [Managing access to larger runners](https://docs.github.com/en/actions/using-github-hosted-runners/managing-access-to-larger-runners)

## Alternative Approach

If larger runners are not available or desired, consider:

1. **Keep ubuntu-latest** - Maintain current configuration
2. **Self-hosted runners** - Set up custom runners with desired specs
3. **Partial migration** - Use larger runners only for compute-intensive jobs (tests)

## Cost Considerations

Larger runners have higher per-minute costs:
- Standard 2-core: ~$0.008/minute
- 16-core runners: ~$0.128/minute (approximate)

However, faster execution may offset the cost:
- If jobs run 50% faster, the cost increase may be negligible
- Reduced developer wait time has value
- See `PERFORMANCE_COMPARISON.md` for detailed analysis once available

## Testing the Configuration

Once the runner is configured:

1. Push any commit to this PR branch
2. The workflow should start automatically
3. Verify jobs are assigned to the correct runner
4. Monitor execution times
5. Update `PERFORMANCE_COMPARISON.md` with results

## Questions?

Contact:
- GitHub organization administrator
- GitHub support for runner configuration questions
- See GitHub's support documentation for enterprise customers
