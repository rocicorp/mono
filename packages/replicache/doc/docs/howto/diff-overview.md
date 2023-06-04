---
title: Overview
slug: /howto/diff/overview
---

# Diff Strategies

The Replicache protocol leaves a lot of flexibility in how servers can calculate the diff to return in the [pull endpoint](/reference/server-pull). This section summarizes some common strategies, and their tradeoffs.

Our general recommendation is to start with the [Global Version](/howto/diff/global-version) strategy and move to [Row Versioning](/howto/diff/row-version) when you need either increased flexibility or throughput.

<table>
    <thead>
        <tr>
            <th>Strategy</th>
            <th>Correct?</th>
            <th>Performance</th>
            <th>Implementation</th>
            <th>Flexibility</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td align="center" nowrap="true"><a href="/howto/diff/reset"><b>Reset</b></a></td>
            <td align="center">✅</td>
            <td>😅 Really just useful for understanding Replicache or for very tiny applications.</td>
            <td align="center" nowrap="true">👍🏼 Trivial</td>
            <td>👍🏼 Very flexible – supports deletes, auth changes automatically.</td>
        </tr>
        <tr>
            <td align="center" nowrap="true"><a href="/howto/diff/global-version"><b>Global Version</b></a></td>
            <td align="center">✅</td>
            <td>🤷🏻 Limits write throughput across application to about 50 pushes/second</td>
            <td align="center" nowrap="true">👍🏼 Trivial</td>
            <td>🤷🏻 Requires soft-deletes and special care to support auth changes and incremental sync.</td>
        </tr>
        <tr>
            <td align="center" nowrap="true"><a href="/howto/diff/per-space-version"><b>Per-Space Version</b></a></td>
            <td align="center">✅</td>
            <td>🤷🏻 Limits write throughput per-space to about 50 pushes/second</td>
            <td align="center" nowrap="true">🤷🏻 Moderately difficult</td>
            <td>😅 Same issues as global version, plus must partition data into spaces.</td>
        </tr>
        <tr>
            <td align="center" nowrap="true"><a href="/howto/diff/row-version"><b>Row Versioning</b></a></td>
            <td align="center">✅</td>
            <td>👍🏼 Increased read and write load in pull, but no contention anywhere so quite scalable</td>
            <td align="center" nowrap="true">😅 Most difficult</td>
            <td>👍🏼 Very flexible – supports deletes, auth changes, and incremental sync easily.</td>
        </tr>
    </tbody>
</table>
