"""Summarize ZeroLogLeak SARIF results and fail the run when a leak is found.

Writes a markdown table to stdout and, when running under Actions, to the job
summary. The CodeQL step logs are extractor chatter and never show the alerts
themselves, so without this a run reads as silence either way.
"""

import argparse
import glob
import json
import os
import re
import sys

# SARIF messages embed `[label](n)`, where n indexes the result's
# relatedLocations. Left alone it renders as a link to nowhere.
EMBEDDED_LINK = re.compile(r'\[([^\]]+)\]\((\d+)\)')


def location(result):
    physical = (result.get('locations') or [{}])[0].get('physicalLocation', {})
    uri = physical.get('artifactLocation', {}).get('uri', '?')
    line = physical.get('region', {}).get('startLine', '?')
    return uri, line


def blob_url():
    """Base URL for linking a path into the tree, when running under Actions."""
    server = os.environ.get('GITHUB_SERVER_URL')
    repository = os.environ.get('GITHUB_REPOSITORY')
    sha = os.environ.get('GITHUB_SHA')
    if server and repository and sha:
        return f'{server}/{repository}/blob/{sha}'
    return None


def related_locations(result):
    """Maps each relatedLocation id to the place it points at."""
    locations = {}
    for location in result.get('relatedLocations') or []:
        identifier = location.get('id')
        physical = location.get('physicalLocation', {})
        uri = physical.get('artifactLocation', {}).get('uri')
        if identifier is not None and uri:
            line = physical.get('region', {}).get('startLine')
            locations[str(identifier)] = (uri, line)
    return locations


def resolve_links(text, locations, base):
    """Rewrites the embedded links to point at the source of the value."""

    def replace(match):
        label, identifier = match.group(1), match.group(2)
        target = locations.get(identifier)
        if not target:
            return label
        uri, line = target
        anchor = f'#L{line}' if line else ''
        if base:
            return f'[{label} ({uri}:{line})]({base}/{uri}{anchor})'
        return f'{label} ({uri}:{line})'

    return EMBEDDED_LINK.sub(replace, text)


def flow_count(result):
    """Number of distinct source-to-sink paths behind one alert.

    Alerts sharing a sink are combined into a single SARIF result carrying one
    message per path, so the alert count alone understates how much there is to
    review.
    """
    messages = len(result.get('message', {}).get('text', '').splitlines())
    return max(len(result.get('codeFlows') or []), messages, 1)


def main(directory):
    alerts = []
    base = blob_url()
    for path in sorted(glob.glob(os.path.join(directory, '*.sarif'))):
        with open(path) as handle:
            sarif = json.load(handle)
        for run in sarif.get('runs', []):
            for result in run.get('results', []):
                uri, line = location(result)
                # A combined message holds one sentence per path; keep the table
                # to one line per alert and let the Flows column carry the rest.
                message = result.get('message', {}).get('text', '').splitlines()[0]
                message = resolve_links(message, related_locations(result), base)
                alerts.append((uri, line, flow_count(result), message.replace('|', r'\|')))

    flows = sum(alert[2] for alert in alerts)
    lines = [
        '# Log leak analysis',
        '',
        'An **alert** is one log call the query flagged. A **flow** is one',
        'source-to-sink path reaching it, so a single call can carry several.',
        '',
        f'## {len(alerts)} alert(s), {flows} flow(s)',
        '',
    ]
    if alerts:
        lines += ['| Location | Flows | Message |', '| --- | --- | --- |']
        for uri, line, count, message in alerts:
            cell = f'{uri}:{line}'
            if base:
                cell = f'[{cell}]({base}/{uri}#L{line})'
            lines.append(f'| {cell} | {count} | {message} |')
        lines += ['', f'**FAILED**: {len(alerts)} leak(s) found across {flows} flow(s).']
    else:
        lines.append('No customer data reaching a log or error message.')

    report = '\n'.join(lines) + '\n'
    sys.stdout.write(report)
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a') as handle:
            handle.write(report)
    return 1 if alerts else 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', nargs='?', default='sarif-results')
    sys.exit(main(parser.parse_args().directory))
