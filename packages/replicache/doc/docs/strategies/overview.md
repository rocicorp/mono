---
title: Overview
slug: /strategies/overview
---

# Backend Strategies

Replicache defines abstract [push](/reference/server-push.md) and [pull](/reference/server-pull.md) endpoints that servers must implement to sync. There are a number of possible strategies to implement these endpoints with different tradeoffs.

The main difference between the strategies is how they calcuate the `patch` required by the pull endpoint. Different approaches to calculating this patch require different state to be stored in the backend database, affect the push and pull implementations, and also some features Replicache chan support.

## Partial Sync

Some strategies easily support syncing only a subset of the data the user has access to, others can do so but it's more effort.

## Dynamic Auth

Some strategies easily support syncing only the subset of the data the user currently has access to, even if that access is indirect via group sharing and so-on.

<br/>
<br/>

<table>
    <thead>
        <tr>
            <th>Strategy</th>
            <th>When to Use</th>
            <th>Push Performance</th>
            <th>Pull Performance</th>
            <th>Implementation</th>
            <th>Partial Sync</th>
            <th>Dynamic Auth</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td align="center" nowrap="true"><a href="/strategies/reset"><b>🤪 Reset</b></a></td>
            <td>For very tiny or slowly-updating applications.</td>
            <td align="center">👍🏼 Little overhead compared to standard web app</td>
            <td align="center">👎🏼 Read and transmit entire client view on each pull</td>
            <td align="center" nowrap="true">👍🏼 Trivial</td>
            <td align="center" nowrap="true">👍🏼 Automatic</td>
            <td align="center" nowrap="true">👍🏼 Automatic</td>
        </tr>
        <tr>
            <td align="center" nowrap="true"><a href="/strategies/global-version"><b>🌏 Global Version</b></a></td>
            <td>Simple apps with low concurrency and no need for partial sync or dynamic auth</td>
            <td align="center">👎🏼 Limited to about 50/second</td>
            <td align="center">👍🏼 Efficient to compute patch</td>
            <td align="center" nowrap="true">👍🏼 Trivial</td>
            <td align="center" nowrap="true">👎🏼 Possible but inefficient.</td>
            <td align="center" nowrap="true">🤷🏻 Extra effort.</td>
        </tr>
        <tr>
            <td align="center" nowrap="true"><a href="/strategies/per-space-version"><b>🛸 Per-Space Version</b></a></td>
            <td>Simple apps that can be partitioned easily along some boundary like organization or account</td>
            <td align="center">👎🏼 Limited to about 50/second/space</td>
            <td align="center">👍🏼 Efficient to compute patch</td>
            <td align="center" nowrap="true">👍🏼 Trivial</td>
            <td align="center" nowrap="true">👎🏼 Possible but inefficient.</td>
            <td align="center" nowrap="true">🤷🏻 Extra effort.</td>
        </tr>
        <tr>
            <td align="center" nowrap="true"><a href="/strategies/row-version"><b>🚣 Row Versioning</b></a></td>
            <td>Apps that need greater concurrency, partial sync, or dynamic auth</td>
            <td align="center">👍🏼 Little overhead compared to standard web app</td>
            <td align="center">👍🏼 More overhead than standard web app but scales well</td>
            <td align="center" nowrap="true">🤷🏻 Moderately difficult</td>
            <td align="center" nowrap="true">👍🏼 Automatic</td>
            <td align="center" nowrap="true">👍🏼 Automatic</td>
        </tr>
    </tbody>
</table>
