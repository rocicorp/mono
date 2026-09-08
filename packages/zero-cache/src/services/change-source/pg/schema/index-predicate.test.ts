import {describe, expect, test} from 'vitest';
import * as PostgresTypeClass from '../../../../db/postgres-type-class-enum.ts';
import type {PublishedTableSpec} from '../../../../db/specs.ts';
import {translatePostgresIndexPredicate} from './index-predicate.ts';

function table(
  columns: Readonly<
    Record<
      string,
      {
        readonly dataType: string;
        readonly pgTypeClass?: string | undefined;
        readonly collationIsDeterministic?: boolean | undefined;
      }
    >
  >,
) {
  return {
    oid: 1,
    schema: 'public',
    name: 'item',
    columns: Object.fromEntries(
      Object.entries(columns).map(([name, column], pos) => [
        name,
        {
          pos: pos + 1,
          typeOID: pos + 1,
          notNull: false,
          dflt: null,
          ...column,
        },
      ]),
    ),
    publications: {},
  } as unknown as PublishedTableSpec & {
    readonly columns: Readonly<
      Record<
        string,
        PublishedTableSpec['columns'][string] & {
          readonly collationIsDeterministic?: boolean | undefined;
        }
      >
    >;
  };
}

const supportedTable = table({
  active: {dataType: 'bool'},
  archived: {dataType: 'bool'},
  attempts: {dataType: 'int4'},
  large: {dataType: 'int8'},
  status: {dataType: 'varchar', collationIsDeterministic: true},
  deleted_at: {dataType: 'timestamptz'},
  kind: {dataType: 'issue_kind', pgTypeClass: PostgresTypeClass.Enum},
  id: {dataType: 'uuid'},
});

describe('PostgreSQL partial-index predicate translation', () => {
  test('canonicalizes supported Boolean combinations', () => {
    expect(
      translatePostgresIndexPredicate(
        `(active AND (NOT archived) AND attempts != +0005 ` +
          `AND status = E'it\\'s \\\\'::text AND deleted_at IS NULL)`,
        supportedTable,
      ),
    ).toEqual({
      supported: true,
      predicate: {
        type: 'and',
        conditions: [
          {
            type: 'comparison',
            column: 'active',
            op: '=',
            value: {type: 'boolean', value: true},
          },
          {
            type: 'comparison',
            column: 'archived',
            op: '=',
            value: {type: 'boolean', value: false},
          },
          {
            type: 'comparison',
            column: 'attempts',
            op: '<>',
            value: {type: 'integer', value: '5'},
          },
          {
            type: 'comparison',
            column: 'status',
            op: '=',
            value: {type: 'string', value: `it's \\`},
          },
          {type: 'null-test', column: 'deleted_at', op: 'IS NULL'},
        ],
      },
    });
  });

  test('accepts bounds, safe casts, enum, and UUID values', () => {
    expect(
      translatePostgresIndexPredicate(
        `large >= '-9223372036854775808'::bigint OR ` +
          `(status)::text = 'open'::text OR ` +
          `kind = 'issue'::public.issue_kind OR ` +
          `id = '123e4567-e89b-12d3-a456-426614174000'::uuid`,
        supportedTable,
      ).supported,
    ).toBe(true);
    expect(
      translatePostgresIndexPredicate(
        `large <= '9223372036854775807'::bigint`,
        supportedTable,
      ),
    ).toMatchObject({
      supported: true,
      predicate: {value: {type: 'integer', value: '9223372036854775807'}},
    });
  });

  test('decodes PostgreSQL escape strings and safe casts in null tests', () => {
    expect(
      translatePostgresIndexPredicate(
        String.raw`status = E'\141\x62\u0063'::text AND (status)::text IS NOT NULL`,
        supportedTable,
      ),
    ).toMatchObject({
      supported: true,
      predicate: {
        conditions: [
          {value: {type: 'string', value: 'abc'}},
          {type: 'null-test', column: 'status', op: 'IS NOT NULL'},
        ],
      },
    });
  });

  test.each([
    `status IN ('in_progress', 'want_to_read')`,
    `status = ANY (ARRAY['in_progress'::character varying, ` +
      `'want_to_read'::character varying]::character varying[])`,
  ])('translates a literal list: %s', sql => {
    expect(translatePostgresIndexPredicate(sql, supportedTable)).toEqual({
      supported: true,
      predicate: {
        type: 'or',
        conditions: [
          {
            type: 'comparison',
            column: 'status',
            op: '=',
            value: {type: 'string', value: 'in_progress'},
          },
          {
            type: 'comparison',
            column: 'status',
            op: '=',
            value: {type: 'string', value: 'want_to_read'},
          },
        ],
      },
    });
  });

  test.each([
    [`missing = 1`, 'unpublished-column'],
    [`lower(status) = 'open'`, 'unsupported-function'],
    [`coalesce(status, 'open') = 'x'`, 'unsupported-function'],
    [`attempts = large`, 'unsupported-syntax'],
    [`attempts::int2 = 1`, 'unsupported-cast'],
    [`status < 'open'`, 'unsupported-operator'],
    [`status COLLATE "C" = 'open'`, 'non-deterministic-collation'],
    [`status ~ '^open'`, 'unsupported-operator'],
  ] as const)('rejects %s as %s', (sql, reason) => {
    expect(translatePostgresIndexPredicate(sql, supportedTable)).toEqual({
      supported: false,
      reason,
    });
  });

  test('rejects comparisons under a nondeterministic collation', () => {
    expect(
      translatePostgresIndexPredicate(
        `status = 'open'`,
        table({
          status: {
            dataType: 'text',
            collationIsDeterministic: false,
          },
        }),
      ),
    ).toEqual({
      supported: false,
      reason: 'non-deterministic-collation',
    });
  });
});
