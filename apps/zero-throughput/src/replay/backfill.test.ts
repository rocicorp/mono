import {describe, expect, test} from 'vitest';
import {
  HeldRowTracker,
  pageStatement,
  parseSchedule,
  quoteTable,
  scheduleSeconds,
  segmentAt,
  type BackfillSpec,
} from './backfill.ts';

const spec: BackfillSpec = {
  column: 'language',
  from: 'en-US',
  to: 'en',
  set: 'updated_at = now()',
  tables: [
    {
      table: 'catalog.work_titles',
      cursorColumn: 'title_id',
      conflict:
        'EXISTS (SELECT 1 FROM catalog.work_titles o WHERE o.work_id = t.work_id AND o.language = {{to}})',
    },
    {
      table: 'catalog.work_descriptions',
      cursorColumn: 'work_id',
    },
  ],
};

describe('parseSchedule', () => {
  test('parses rate:seconds segments', () => {
    const schedule = parseSchedule('0:600, 24:1200,0.5:30');
    expect(schedule).toEqual([
      {rowsPerSecond: 0, seconds: 600},
      {rowsPerSecond: 24, seconds: 1200},
      {rowsPerSecond: 0.5, seconds: 30},
    ]);
    expect(scheduleSeconds(schedule)).toBe(1830);
  });

  test('rejects malformed segments', () => {
    expect(() => parseSchedule('24')).toThrow('Invalid schedule segment');
    expect(() => parseSchedule('-1:10')).toThrow('Invalid schedule segment');
    expect(() => parseSchedule('5:0')).toThrow('Invalid schedule segment');
  });
});

describe('segmentAt', () => {
  const schedule = parseSchedule('0:10,24:20');
  test('finds the segment in effect and its end', () => {
    expect(segmentAt(schedule, 0)).toEqual({
      segment: {rowsPerSecond: 0, seconds: 10},
      endMs: 10_000,
    });
    expect(segmentAt(schedule, 10_000)?.segment.rowsPerSecond).toBe(24);
    expect(segmentAt(schedule, 29_999)?.endMs).toBe(30_000);
    expect(segmentAt(schedule, 30_000)).toBeUndefined();
  });
});

describe('pageStatement', () => {
  test('keeps the guards out of the page query', () => {
    const sql = pageStatement(spec, spec.tables[0], {
      from: 'en-US',
      to: 'en',
      after: null,
      pageSize: 100,
      validText: '(check_min_length(title, 1))',
    });
    const [pageCTE, update] = sql.split('), updated AS (');
    expect(pageCTE).toContain('FROM "catalog"."work_titles" t');
    expect(pageCTE).toContain(`t."language" = 'en-US'`);
    expect(pageCTE).toContain('LIMIT 100');
    expect(pageCTE).not.toContain('EXISTS');
    expect(pageCTE).not.toContain('check_min_length');
    expect(update).toContain(`SET "language" = 'en', updated_at = now()`);
    expect(update).toContain(
      `NOT (EXISTS (SELECT 1 FROM catalog.work_titles o WHERE o.work_id = t.work_id AND o.language = 'en'))`,
    );
    expect(update).toContain('AND ((check_min_length(title, 1)))');
    expect(update).toContain('RETURNING t."title_id"::text AS k');
  });

  test('pages after the cursor and defaults the guards', () => {
    const sql = pageStatement(spec, spec.tables[1], {
      from: 'en',
      to: 'en-US',
      after: "a'b",
      pageSize: 5,
    });
    expect(sql).toContain(`t."work_id" > 'a''b'::uuid`);
    expect(sql).toContain('AND NOT (false)');
    expect(sql).toContain('AND (true)');
    expect(sql).toContain(`SET "language" = 'en-US'`);
  });
});

test('quoteTable quotes each part', () => {
  expect(quoteTable('catalog.work_titles')).toBe('"catalog"."work_titles"');
  expect(quoteTable('languages')).toBe('"languages"');
});

describe('HeldRowTracker', () => {
  test('times rows carrying the new value back to their commit', () => {
    let now = 1_000;
    const delivered: number[] = [];
    const tracker = new HeldRowTracker({
      spec,
      to: 'en',
      now: () => now,
      ttlMs: 60_000,
      onDelivered: ms => delivered.push(ms),
    });
    tracker.written('catalog.work_titles', ['t1', 't2']);
    now = 1_250;
    tracker.observe('catalog.work_titles', {title_id: 't1', language: 'en'});
    tracker.observe('catalog.work_titles', {title_id: 't2', language: 'en-US'});
    tracker.observe('catalog.work_titles', {title_id: 't3', language: 'en'});
    tracker.observe('catalog.works', {title_id: 't1', language: 'en'});
    expect(delivered).toEqual([250]);

    now = 100_000;
    tracker.expire();
    tracker.observe('catalog.work_titles', {title_id: 't1', language: 'en'});
    expect(delivered).toEqual([250]);
  });
});
