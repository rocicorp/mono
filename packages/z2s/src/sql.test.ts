import {describe, expect, test} from 'vitest';
import {
  formatPg,
  formatPgInternalConvert,
  sql,
  sqlConvertColumnArg,
  sqlConvertSingularLiteralArg,
} from './sql.ts';

test('identical values result in a single placeholder', () => {
  const userId = 1;
  expect(
    formatPg(
      sql`SELECT * FROM "user" WHERE "id" = ${userId} AND "id" = ${userId}`,
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT * FROM "user" WHERE "id" = $1 AND "id" = $1",
      "values": [
        1,
      ],
    }
  `);

  const str = JSON.stringify({a: 1});
  expect(
    formatPg(
      sql`SELECT * FROM "user" WHERE "meta1" = ${str} AND "id" = ${userId} OR "meta2" = ${str} OR "otherId" = ${userId} AND foo = ${''}`,
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT * FROM "user" WHERE "meta1" = $1 AND "id" = $2 OR "meta2" = $1 OR "otherId" = $2 AND foo = $3",
      "values": [
        "{"a":1}",
        1,
        "",
      ],
    }
  `);
});

describe('string arg packing', () => {
  test('single arg', () => {
    expect(
      formatPgInternalConvert(
        sql`SELECT * FROM "user" WHERE "id" = ${sqlConvertColumnArg(
          {
            isArray: false,
            isEnum: false,
            type: 'numeric',
          },
          1,
          false,
          true,
        )} `,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "user" WHERE "id" = $1::text::double precision",
        "values": [
          "1",
        ],
      }
    `);
  });

  // identical values should only be encoded once
  test('many equivalent args', () => {
    expect(
      formatPgInternalConvert(
        sql`SELECT * FROM "user" WHERE "id" = ${sqlConvertColumnArg(
          {
            isArray: false,
            isEnum: false,
            type: 'numeric',
          },
          1,
          false,
          true,
        )} OR "other_id" = ${sqlConvertColumnArg(
          {
            isArray: false,
            isEnum: false,
            type: 'numeric',
          },
          1,
          false,
          true,
        )}`,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "user" WHERE "id" = $1::text::double precision OR "other_id" = $1::text::double precision",
        "values": [
          "1",
        ],
      }
    `);
  });

  describe('many types', () => {
    test('jsonb type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "jsonb" = ${sqlConvertColumnArg(
            {
              isArray: false,
              isEnum: false,
              type: 'jsonb',
            },
            {},
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "jsonb" = $1::text::jsonb",
        "values": [
          "{}",
        ],
      }
    `);
    });

    test('numeric type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "numeric" = ${sqlConvertColumnArg(
            {
              isArray: false,
              isEnum: false,
              type: 'numeric',
            },
            1,
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "numeric" = $1::text::double precision",
        "values": [
          "1",
        ],
      }
    `);
    });

    test('text type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "str" = ${sqlConvertColumnArg(
            {
              isArray: false,
              isEnum: false,
              type: 'text',
            },
            'str',
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "str" = $1::text COLLATE "ucs_basic"",
        "values": [
          "str",
        ],
      }
    `);
    });

    test('boolean type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "boolean" = ${sqlConvertColumnArg(
            {
              isArray: false,
              isEnum: false,
              type: 'boolean',
            },
            true,
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "boolean" = $1::text::boolean",
        "values": [
          "true",
        ],
      }
    `);
    });

    test('uuid type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "uuid"::text = ${sqlConvertColumnArg(
            {
              isArray: false,
              isEnum: false,
              type: 'uuid',
            },
            '8f1dceb2-b3dd-46cf-9deb-460e9d87541c',
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "uuid"::text = $1::text COLLATE "ucs_basic"",
        "values": [
          "8f1dceb2-b3dd-46cf-9deb-460e9d87541c",
        ],
      }
    `);
    });

    test('enum type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "enum"::text = ${sqlConvertColumnArg(
            {
              isArray: false,
              isEnum: true,
              type: 'some_enum',
            },
            'ENUM_KEY',
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "enum"::text = $1::text COLLATE "ucs_basic"",
        "values": [
          "ENUM_KEY",
        ],
      }
    `);
    });

    test('timestamp type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "timestamp" = ${sqlConvertColumnArg(
            {
              isArray: false,
              isEnum: false,
              type: 'timestamp',
            },
            1712345678901,
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "timestamp" = to_timestamp($1::text::bigint / 1000.0) AT TIME ZONE 'UTC'",
        "values": [
          "1712345678901",
        ],
      }
    `);
    });

    test('timestamptz type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "timestampz" = ${sqlConvertColumnArg(
            {
              isArray: false,
              isEnum: false,
              type: 'timestamptz',
            },
            1712345678901,
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "timestampz" = to_timestamp($1::text::bigint / 1000.0)",
        "values": [
          "1712345678901",
        ],
      }
    `);
    });

    test('jsonb[] type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "jsonb" = ${sqlConvertColumnArg(
            {
              isArray: true,
              isEnum: false,
              type: 'jsonb',
            },
            [{}, {a: 1}],
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "jsonb" = ARRAY(
                SELECT value::text::jsonb FROM jsonb_array_elements_text($1::text::jsonb)
              )",
        "values": [
          "[{},{"a":1}]",
        ],
      }
    `);
    });

    test('numeric[] type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "numeric" = ${sqlConvertColumnArg(
            {
              isArray: true,
              isEnum: false,
              type: 'numeric',
            },
            [1, 2.2, 3],
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "numeric" = ARRAY(
                SELECT value::text::double precision FROM jsonb_array_elements_text($1::text::jsonb)
              )",
        "values": [
          "[1,2.2,3]",
        ],
      }
    `);
    });

    test('text[] type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "str" = ${sqlConvertColumnArg(
            {
              isArray: true,
              isEnum: false,
              type: 'text',
            },
            ['a', 'b', 'c'],
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "str" = ARRAY(
                SELECT value::text COLLATE "ucs_basic" FROM jsonb_array_elements_text($1::text::jsonb)
              )",
        "values": [
          "["a","b","c"]",
        ],
      }
    `);
    });

    test('boolean[] type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "boolean" = ${sqlConvertColumnArg(
            {
              isArray: true,
              isEnum: false,
              type: 'boolean',
            },
            [true, false, true],
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "boolean" = ARRAY(
                SELECT value::text::boolean FROM jsonb_array_elements_text($1::text::jsonb)
              )",
        "values": [
          "[true,false,true]",
        ],
      }
    `);
    });

    test('uuid[] type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "uuid"::text = ${sqlConvertColumnArg(
            {
              isArray: true,
              isEnum: false,
              type: 'uuid',
            },
            [
              '8f1dceb2-b3dd-46cf-9deb-460e9d87541c',
              '11111111-1111-1111-1111-111111111111',
            ],
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "uuid"::text = ARRAY(
                SELECT value::text COLLATE "ucs_basic" FROM jsonb_array_elements_text($1::text::jsonb)
              )",
        "values": [
          "["8f1dceb2-b3dd-46cf-9deb-460e9d87541c","11111111-1111-1111-1111-111111111111"]",
        ],
      }
    `);
    });

    test('enum[] type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "enum"::text = ${sqlConvertColumnArg(
            {
              isArray: true,
              isEnum: true,
              type: 'some_enum',
            },
            ['ENUM_KEY', 'OTHER_KEY'],
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "enum"::text = ARRAY(
                SELECT value::text COLLATE "ucs_basic" FROM jsonb_array_elements_text($1::text::jsonb)
              )",
        "values": [
          "["ENUM_KEY","OTHER_KEY"]",
        ],
      }
    `);
    });

    test('timestamp[] type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "timestamp" = ${sqlConvertColumnArg(
            {
              isArray: true,
              isEnum: false,
              type: 'timestamp',
            },
            [1712345678901, 1712345678902],
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "timestamp" = ARRAY(
                SELECT to_timestamp(value::text::bigint / 1000.0) AT TIME ZONE 'UTC' FROM jsonb_array_elements_text($1::text::jsonb)
              )",
        "values": [
          "[1712345678901,1712345678902]",
        ],
      }
    `);
    });

    test('timestamptz[] type', () => {
      expect(
        formatPgInternalConvert(
          sql`SELECT * FROM "foo" WHERE "timestampz" = ${sqlConvertColumnArg(
            {
              isArray: true,
              isEnum: false,
              type: 'timestamptz',
            },
            [1712345678901, 1712345678902],
            false,
            true,
          )}`,
        ),
      ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE "timestampz" = ARRAY(
                SELECT to_timestamp(value::text::bigint / 1000.0) FROM jsonb_array_elements_text($1::text::jsonb)
              )",
        "values": [
          "[1712345678901,1712345678902]",
        ],
      }
    `);
    });
  });

  test('mapped and joined', () => {
    const values = [1, 1.1, 'two', true, null];
    expect(
      formatPgInternalConvert(
        sql`SELECT * FROM "foo" WHERE ${sql.join(
          values.map(v => sqlConvertSingularLiteralArg(v)),
          ' AND ',
        )} `,
      ),
    ).toMatchInlineSnapshot(`
      {
        "text": "SELECT * FROM "foo" WHERE $1::text::double precision AND $2::text::double precision AND $3::text::text COLLATE "ucs_basic" AND $4::text::boolean AND $5",
        "values": [
          "1",
          "1.1",
          "two",
          "true",
          null,
        ],
      }
    `);
  });

  describe('insert', () => {
    test('insert numeric', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: false, isEnum: false, type: 'numeric'},
            1,
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES ($1::text::numeric)",
          "values": [
            "1",
          ],
        }
      `);
    });

    test('insert numeric[]', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: true, isEnum: false, type: 'numeric'},
            [1, 2, 3],
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES (ARRAY(
                  SELECT value::text::numeric FROM jsonb_array_elements_text($1::text::jsonb)
                ))",
          "values": [
            "[1,2,3]",
          ],
        }
      `);
    });

    test('insert text', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: false, isEnum: false, type: 'text'},
            'two',
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES ($1::text::text)",
          "values": [
            "two",
          ],
        }
      `);
    });

    test('insert text[]', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: true, isEnum: false, type: 'text'},
            ['a', 'b', 'c'],
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES (ARRAY(
                  SELECT value::text::text FROM jsonb_array_elements_text($1::text::jsonb)
                ))",
          "values": [
            "["a","b","c"]",
          ],
        }
      `);
    });

    test('insert boolean', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: false, isEnum: false, type: 'boolean'},
            true,
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES ($1::text::boolean)",
          "values": [
            "true",
          ],
        }
      `);
    });

    test('insert boolean[]', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: true, isEnum: false, type: 'boolean'},
            [true, false],
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES (ARRAY(
                  SELECT value::text::boolean FROM jsonb_array_elements_text($1::text::jsonb)
                ))",
          "values": [
            "[true,false]",
          ],
        }
      `);
    });

    test('insert jsonb', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: false, isEnum: false, type: 'jsonb'},
            {a: 1},
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES ($1::text::jsonb)",
          "values": [
            "{"a":1}",
          ],
        }
      `);
    });

    test('insert jsonb[]', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: true, isEnum: false, type: 'jsonb'},
            [{}, {a: 1}],
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES (ARRAY(
                  SELECT value::text::jsonb FROM jsonb_array_elements_text($1::text::jsonb)
                ))",
          "values": [
            "[{},{"a":1}]",
          ],
        }
      `);
    });

    test('insert enum', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: false, isEnum: true, type: 'some_enum'},
            'ENUM_KEY',
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES ($1::text::"some_enum")",
          "values": [
            "ENUM_KEY",
          ],
        }
      `);
    });

    test('insert enum[]', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: true, isEnum: true, type: 'some_enum'},
            ['ENUM_KEY', 'OTHER_KEY'],
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES (ARRAY(
                  SELECT value::text::"some_enum" FROM jsonb_array_elements_text($1::text::jsonb)
                ))",
          "values": [
            "["ENUM_KEY","OTHER_KEY"]",
          ],
        }
      `);
    });

    test('insert uuid', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: false, isEnum: false, type: 'uuid'},
            '8f1dceb2-b3dd-46cf-9deb-460e9d87541c',
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES ($1::text::uuid)",
          "values": [
            "8f1dceb2-b3dd-46cf-9deb-460e9d87541c",
          ],
        }
      `);
    });

    test('insert uuid[]', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: true, isEnum: false, type: 'uuid'},
            [
              '8f1dceb2-b3dd-46cf-9deb-460e9d87541c',
              '11111111-1111-1111-1111-111111111111',
            ],
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES (ARRAY(
                  SELECT value::text::uuid FROM jsonb_array_elements_text($1::text::jsonb)
                ))",
          "values": [
            "["8f1dceb2-b3dd-46cf-9deb-460e9d87541c","11111111-1111-1111-1111-111111111111"]",
          ],
        }
      `);
    });

    test('insert timestamp', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: false, isEnum: false, type: 'timestamp'},
            1712345678901, // JS epoch ms
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES (to_timestamp($1::text::bigint / 1000.0) AT TIME ZONE 'UTC')",
          "values": [
            "1712345678901",
          ],
        }
      `);
    });

    test('insert timestamp[]', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: true, isEnum: false, type: 'timestamp'},
            [1712345678901, 1712345678902],
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES (ARRAY(
                  SELECT to_timestamp(value::text::bigint / 1000.0) AT TIME ZONE 'UTC' FROM jsonb_array_elements_text($1::text::jsonb)
                ))",
          "values": [
            "[1712345678901,1712345678902]",
          ],
        }
      `);
    });

    test('insert timestamptz', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: false, isEnum: false, type: 'timestamptz'},
            1712345678901, // JS epoch ms
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES (to_timestamp($1::text::bigint / 1000.0))",
          "values": [
            "1712345678901",
          ],
        }
      `);
    });

    test('insert timestamptz[]', () => {
      expect(
        formatPgInternalConvert(
          sql`INSERT INTO "foo" VALUES (${sqlConvertColumnArg(
            {isArray: true, isEnum: false, type: 'timestamptz'},
            [1712345678901, 1712345678902],
            false,
            false,
          )})`,
        ),
      ).toMatchInlineSnapshot(`
        {
          "text": "INSERT INTO "foo" VALUES (ARRAY(
                  SELECT to_timestamp(value::text::bigint / 1000.0) FROM jsonb_array_elements_text($1::text::jsonb)
                ))",
          "values": [
            "[1712345678901,1712345678902]",
          ],
        }
      `);
    });
  });
});
