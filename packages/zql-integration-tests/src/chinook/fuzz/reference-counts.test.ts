import {expect, test} from 'vitest';
import {must} from '../../../../shared/src/must.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import {buildPipeline} from '../../../../zql/src/builder/builder.ts';
import {TestBuilderDelegate} from '../../../../zql/src/builder/test-builder-delegate.ts';
import {
  makeChildChange,
  makeEditChange,
  makeRemoveChange,
} from '../../../../zql/src/ivm/change.ts';
import {MemorySource} from '../../../../zql/src/ivm/memory-source.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../../../../zql/src/ivm/source.ts';
import {consume} from '../../../../zql/src/ivm/stream.ts';
import {asQueryInternals} from '../../../../zql/src/query/query-internals.ts';
import {schema} from '../schema.ts';
import {pkOf} from './axes.ts';
import {flipVariants} from './flip.ts';
import {Data} from './literals.ts';
import {miniData} from './mini.ts';
import {pushForQuery, type Mutation} from './push.ts';
import {ReferenceCounts} from './reference-counts.ts';
import {enumerate, label, lower} from './skeleton.ts';

function sourcesFor(rows: Record<string, readonly Row[]>) {
  return Object.fromEntries(
    Object.entries(schema.tables).map(([name, table]) => {
      const source = new MemorySource(name, table.columns, table.primaryKey);
      for (const row of rows[name] ?? []) {
        consume(source.push(makeSourceChangeAdd(row)));
      }
      return [name, source];
    }),
  );
}

function walk(ast: AST, mutations: readonly Mutation[], fetchOnPush: boolean) {
  const sources = sourcesFor(miniData);
  const observed = new ReferenceCounts(
    buildPipeline(ast, new TestBuilderDelegate(sources, false, true), 'live'),
    fetchOnPush,
  );
  try {
    for (const [step, mutation] of mutations.entries()) {
      const source = must(sources[mutation.table]);
      if (mutation.kind === 'add') {
        consume(source.push(makeSourceChangeAdd(mutation.row)));
      } else if (mutation.kind === 'edit') {
        consume(source.push(makeSourceChangeEdit(mutation.row, mutation.old)));
      } else {
        consume(source.push(makeSourceChangeRemove(mutation.row)));
      }
      observed.check(`step ${step}: ${mutation.kind} ${mutation.table}`);
    }
  } finally {
    observed.destroy();
  }
}

const data = new Data(miniData, pkOf);

test('reference checker tracks repeated children, child changes, and primary-key edits', () => {
  const ast: AST = {
    table: 'album',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['artistId'], childField: ['id']},
        subquery: {table: 'artist', alias: 'artist', orderBy: [['id', 'asc']]},
      },
    ],
  };
  const counts = new ReferenceCounts(
    buildPipeline(ast, new TestBuilderDelegate(sourcesFor(miniData)), 'test'),
  );
  try {
    expect(counts.counts.get('["artist",[1]]')).toBe(2);
    const album = {row: miniData.album[0], relationships: {}};
    const artist = {row: miniData.artist[0], relationships: {}};
    counts.push(
      makeChildChange(album, {
        relationshipName: 'artist',
        change: makeRemoveChange(artist),
      }),
    );
    expect(counts.counts.get('["artist",[1]]')).toBe(1);
    counts.push(
      makeEditChange({row: {...album.row, id: 99}, relationships: {}}, album),
    );
    expect(counts.counts.has('["album",[10]]')).toBe(false);
    expect(counts.counts.get('["album",[99]]')).toBe(1);
    counts.push(
      makeRemoveChange({
        row: miniData.album[1],
        relationships: {artist: () => [artist]},
      }),
    );
    expect(counts.counts.has('["artist",[1]]')).toBe(false);
  } finally {
    counts.destroy();
  }
});

test('reference checker agrees with fetch for unfiltered related pushes', () => {
  for (const skel of enumerate({depth: 1, related: 1, exists: 0})) {
    const ast = asQueryInternals(lower(skel)).ast;
    walk(ast, pushForQuery(data, skel, ast, 2), false);
  }
});

test('generated push reference counts', () => {
  const fetchOnPush = false;
  const failures: string[] = [];
  for (const skel of enumerate({depth: 1, related: 0, exists: 1})) {
    if (skel.children.length === 0) {
      continue;
    }
    const base = asQueryInternals(lower(skel)).ast;
    for (const [plan, ast] of [
      ['default', base] as const,
      ...flipVariants(base, 3),
    ]) {
      for (const limit of [undefined, 1, 2]) {
        const query = {...ast, limit};
        const mutations = pushForQuery(data, skel, query, 2);
        try {
          walk(query, mutations, fetchOnPush);
        } catch (e) {
          failures.push(`${label(skel)}|${plan}|limit=${limit}: ${String(e)}`);
        }
      }
    }
  }
  if (failures.length) {
    throw new Error(
      `${failures.length} cases failed:\n${failures.slice(0, 12).join('\n')}\nAll failing cases:\n${failures.map(f => f.split(': ')[0]).join('\n')}`,
    );
  }
}, 60_000);
