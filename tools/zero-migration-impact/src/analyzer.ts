// oxlint-disable e18e/prefer-static-regex

import {readFile, readdir, stat} from 'node:fs/promises';
import {join} from 'node:path';
import {
  findFirstTopLevelKeyword,
  findTopLevelKeyword,
  normalizeSql,
  readIdentifier,
  readQualifiedIdentifier,
  skipWhitespace,
  splitSqlStatements,
  splitTopLevel,
  type SqlStatement,
} from './sql.ts';
import {
  profileForZeroVersion,
  type ZeroVersionProfile,
} from './zero-version.ts';

export type ImpactAnswer = 'no' | 'possible' | 'yes';
export type LagRisk = 'low' | 'medium' | 'high';
export type Severity = 'info' | 'warning' | 'error';
export type Safety = 'safe' | 'review' | 'unsafe';

export type FindingImpact = {
  backfill: ImpactAnswer;
  schemaVersionNotSupported: ImpactAnswer;
  replicationLag: LagRisk;
};

export type FindingLocation = {
  file?: string | undefined;
  startLine: number;
  endLine: number;
  statementIndex: number;
};

export type Finding = {
  id: string;
  title: string;
  severity: Severity;
  location: FindingLocation;
  statement: string;
  details: string;
  impact: FindingImpact;
  remediations: string[];
};

export type ImpactSummary = {
  safety: Safety;
  backfill: ImpactAnswer;
  schemaVersionNotSupported: ImpactAnswer;
  replicationLag: LagRisk;
};

export type AnalysisResult = {
  summary: ImpactSummary;
  zeroVersion: ZeroVersionProfile;
  files: string[];
  statementsAnalyzed: number;
  findings: Finding[];
};

export type AnalysisOptions = {
  zeroVersion?: string | undefined;
};

type ColumnDefinition = {
  name: string;
  type: string;
  rest: string;
  defaultExpression: string | null;
  notNull: boolean;
  primaryKey: boolean;
  unique: boolean;
};

const TYPE_CONSTRAINT_KEYWORDS = [
  'default',
  'not null',
  'null',
  'primary key',
  'unique',
  'references',
  'check',
  'constraint',
  'generated',
  'collate',
  'identity',
] as const;

const DEFAULT_TERMINATORS = [
  'not null',
  'primary key',
  'unique',
  'references',
  'check',
  'constraint',
  'generated',
  'collate',
  'identity',
] as const;

const SUPPORTED_TYPES = new Map<string, string>([
  ['smallint', 'number'],
  ['integer', 'number'],
  ['int', 'number'],
  ['int2', 'number'],
  ['int4', 'number'],
  ['int8', 'number'],
  ['bigint', 'number'],
  ['smallserial', 'number'],
  ['serial', 'number'],
  ['serial2', 'number'],
  ['serial4', 'number'],
  ['serial8', 'number'],
  ['bigserial', 'number'],
  ['decimal', 'number'],
  ['numeric', 'number'],
  ['real', 'number'],
  ['double precision', 'number'],
  ['float', 'number'],
  ['float4', 'number'],
  ['float8', 'number'],
  ['date', 'number'],
  ['time', 'number'],
  ['timetz', 'number'],
  ['time with time zone', 'number'],
  ['time without time zone', 'number'],
  ['timestamp', 'number'],
  ['timestamptz', 'number'],
  ['timestamp with time zone', 'number'],
  ['timestamp without time zone', 'number'],
  ['bpchar', 'string'],
  ['character', 'string'],
  ['character varying', 'string'],
  ['text', 'string'],
  ['varchar', 'string'],
  ['cidr', 'string'],
  ['ean13', 'string'],
  ['inet', 'string'],
  ['isbn', 'string'],
  ['isbn13', 'string'],
  ['ismn', 'string'],
  ['ismn13', 'string'],
  ['issn', 'string'],
  ['issn13', 'string'],
  ['macaddr', 'string'],
  ['macaddr8', 'string'],
  ['pg_lsn', 'string'],
  ['upc', 'string'],
  ['uuid', 'string'],
  ['bool', 'boolean'],
  ['boolean', 'boolean'],
  ['json', 'json'],
  ['jsonb', 'json'],
]);

export async function analyzeSqlPaths(
  paths: readonly string[],
  options: AnalysisOptions = {},
) {
  const files = await expandSqlPaths(paths);
  const statements: SqlStatement[] = [];
  for (const file of files) {
    statements.push(...splitSqlStatements(await readFile(file, 'utf8'), file));
  }
  return analyzeStatements(
    files,
    statements,
    profileForZeroVersion(options.zeroVersion),
  );
}

export function analyzeSql(
  sql: string,
  file?: string,
  options: AnalysisOptions = {},
) {
  const statements = splitSqlStatements(sql, file);
  return analyzeStatements(
    file ? [file] : [],
    statements,
    profileForZeroVersion(options.zeroVersion),
  );
}

function analyzeStatements(
  files: readonly string[],
  statements: readonly SqlStatement[],
  zeroVersion: ZeroVersionProfile,
): AnalysisResult {
  const findings = statements.flatMap(statement =>
    analyzeStatement(statement, zeroVersion),
  );
  return {
    summary: summarize(findings),
    zeroVersion,
    files: [...files],
    statementsAnalyzed: statements.length,
    findings,
  };
}

async function expandSqlPaths(paths: readonly string[]): Promise<string[]> {
  const files: string[] = [];
  for (const path of paths) {
    const entry = await stat(path);
    if (entry.isDirectory()) {
      files.push(...(await sqlFilesInDirectory(path)));
    } else if (path.toLowerCase().endsWith('.sql')) {
      files.push(path);
    }
  }
  return files.sort();
}

async function sqlFilesInDirectory(dir: string): Promise<string[]> {
  const files: string[] = [];
  for (const entry of await readdir(dir, {withFileTypes: true})) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await sqlFilesInDirectory(path)));
    } else if (entry.isFile() && entry.name.toLowerCase().endsWith('.sql')) {
      files.push(path);
    }
  }
  return files;
}

function analyzeStatement(
  statement: SqlStatement,
  zeroVersion: ZeroVersionProfile,
): Finding[] {
  const sql = normalizeSql(statement.sql);
  if (!sql) {
    return [];
  }

  return [
    ...analyzeCreateTable(statement, sql),
    ...analyzeAlterTable(statement, sql, zeroVersion),
    ...analyzeDropTable(statement, sql),
    ...analyzePublication(statement, sql),
    ...analyzeIndex(statement, sql),
    ...analyzeReplicaIdentity(statement, sql),
    ...analyzeBroadDml(statement, sql),
  ];
}

function analyzeCreateTable(statement: SqlStatement, sql: string): Finding[] {
  if (!/^\s*create\s+(?:temporary\s+|temp\s+|unlogged\s+)?table\b/i.test(sql)) {
    return [];
  }
  const findings: Finding[] = [];
  const table = parseCreateTableName(sql);
  const body = table ? extractParenthesized(sql, table.end) : undefined;

  if (/\bas\s+select\b/i.test(sql)) {
    findings.push(
      finding(statement, {
        id: 'create-table-as-select',
        title: 'CREATE TABLE AS can introduce existing rows',
        severity: 'warning',
        details:
          'Zero skips table backfill for tables introduced by CREATE statements, but CREATE TABLE AS creates data as part of the DDL. Verify that the created table is not published until its data path is understood.',
        impact: {
          backfill: 'possible',
          schemaVersionNotSupported: 'possible',
          replicationLag: 'medium',
        },
        remediations: [
          'Prefer CREATE TABLE without data, add it to the publication while empty, then populate it in batches.',
          'Deploy client code that references the table only after zero-cache has observed the schema.',
        ],
      }),
    );
  }

  if (!body || !table) {
    return findings;
  }

  const parsed = parseCreateTableBody(body);
  addUnsupportedColumnFindings(findings, statement, parsed.columns, table.name);

  const potentialKeys = potentialPrimaryKeys(parsed);
  if (!potentialKeys.length) {
    findings.push(
      finding(statement, {
        id: 'table-without-syncable-key',
        title: `Table ${table.name} may not be syncable by Zero`,
        severity: 'warning',
        details:
          'Zero can only sync a table to clients when the published table has a primary key or a non-null unique index. A client schema that includes this table can get SchemaVersionNotSupported.',
        impact: {
          backfill: 'no',
          schemaVersionNotSupported: 'possible',
          replicationLag: 'low',
        },
        remediations: [
          'Add a primary key, or add a unique index whose columns are all NOT NULL before clients reference the table.',
          'Do not include this table in the Zero client schema until it has a stable syncable key.',
        ],
      }),
    );
  }

  for (const key of potentialKeys) {
    const unsupported = key.filter(columnName => {
      const col = parsed.columns.get(columnName);
      return col ? zqlTypeForPostgresType(col.type) === undefined : false;
    });
    if (unsupported.length) {
      findings.push(
        finding(statement, {
          id: 'unsupported-primary-key-type',
          title: `Table ${table.name} has a key with Zero-unsupported columns`,
          severity: 'warning',
          details: `The key includes ${unsupported.join(', ')}. Zero rejects client schemas for tables whose selected key uses unsupported data types.`,
          impact: {
            backfill: 'no',
            schemaVersionNotSupported: 'possible',
            replicationLag: 'low',
          },
          remediations: [
            'Use Zero-supported key column types such as text, uuid, integers, booleans, or timestamps.',
            'If the upstream primary key must stay unsupported, add a separate non-null unique key with supported column types and use that in the Zero schema.',
          ],
        }),
      );
    }
  }

  return findings;
}

function analyzeAlterTable(
  statement: SqlStatement,
  sql: string,
  zeroVersion: ZeroVersionProfile,
): Finding[] {
  const altered = parseAlterTable(sql);
  if (!altered) {
    return [];
  }

  const findings: Finding[] = [];
  for (const action of splitTopLevel(altered.actions, ',')) {
    analyzeAlterTableAction(
      findings,
      statement,
      altered.table,
      action,
      zeroVersion,
    );
  }
  return findings;
}

function analyzeAlterTableAction(
  findings: Finding[],
  statement: SqlStatement,
  table: string,
  action: string,
  zeroVersion: ZeroVersionProfile,
) {
  const normalized = action.trim();
  if (/^drop\s+constraint\b/i.test(normalized)) {
    const constraint = readIdentifier(
      normalized.replace(/^drop\s+constraint\s+(?:if\s+exists\s+)?/i, ''),
    );
    if (constraint && isLikelyForeignKeyConstraint(constraint.display)) {
      return;
    }
    findings.push(
      finding(statement, {
        id: 'drop-key-constraint',
        title: `Dropping a constraint on ${table} can remove Zero's sync key`,
        severity: 'warning',
        details:
          'If this drops a primary key or non-null unique constraint used by the Zero client schema, zero-cache can reject clients with SchemaVersionNotSupported.',
        impact: {
          backfill: 'no',
          schemaVersionNotSupported: 'possible',
          replicationLag: 'low',
        },
        remediations: [
          'Create the replacement primary key or non-null unique index before dropping the old one.',
          'Confirm the Zero schema primaryKey still matches a non-null unique index after the migration.',
        ],
      }),
    );
    return;
  }

  if (/^add\s+constraint\b/i.test(normalized)) {
    return;
  }

  if (/^add\s+(?:column\b\s+)?/i.test(normalized)) {
    const colSql = normalized.replace(
      /^add\s+(?:column\b\s+)?(?:if\s+not\s+exists\s+)?/i,
      '',
    );
    const column = parseColumnDefinition(colSql);
    if (!column) {
      return;
    }
    addUnsupportedColumnFindings(
      findings,
      statement,
      new Map([[column.name, column]]),
      table,
    );
    if (column.defaultExpression) {
      addColumnDefaultFinding(findings, statement, table, column, zeroVersion);
    }
    if (column.notNull && !column.defaultExpression) {
      findings.push(
        finding(statement, {
          id: 'add-not-null-column-without-default',
          title: `Adding NOT NULL ${table}.${column.name} can block the migration`,
          severity: 'warning',
          details:
            'Postgres rejects ADD COLUMN ... NOT NULL on non-empty tables unless a valid default is supplied. This is not a Zero backfill by itself, but it is a risky migration shape.',
          impact: {
            backfill: 'no',
            schemaVersionNotSupported: 'possible',
            replicationLag: 'low',
          },
          remediations: [
            'Add the column nullable first, backfill application data in batches, then add NOT NULL in a separate migration.',
          ],
        }),
      );
    }
    return;
  }

  if (/^drop\s+(?:column\b\s+)?/i.test(normalized)) {
    const column = readIdentifier(
      normalized.replace(/^drop\s+(?:column\b\s+)?(?:if\s+exists\s+)?/i, ''),
    );
    findings.push(
      finding(statement, {
        id: 'drop-column',
        title: `Dropping ${table}${column ? `.${column.display}` : ' column'} can break active clients`,
        severity: 'warning',
        details:
          'If the current Zero client schema includes this column, zero-cache will reject that client schema with SchemaVersionNotSupported after the column disappears from the published schema.',
        impact: {
          backfill: 'no',
          schemaVersionNotSupported: 'possible',
          replicationLag: 'low',
        },
        remediations: [
          'First deploy clients that no longer read or declare the column in the Zero schema.',
          'Wait for old clients/client groups to age out, then drop the column in a later migration.',
        ],
      }),
    );
    return;
  }

  if (/^rename\s+column\b/i.test(normalized)) {
    const rest = normalized.replace(/^rename\s+column\b/i, '');
    const column = readIdentifier(rest);
    findings.push(
      finding(statement, {
        id: 'rename-column',
        title: `Renaming ${table}${column ? `.${column.display}` : ' column'} is a client schema break`,
        severity: 'warning',
        details:
          'Zero sees the new column name in the published schema. Active clients whose Zero schema still contains the old name can get SchemaVersionNotSupported.',
        impact: {
          backfill: 'no',
          schemaVersionNotSupported: 'possible',
          replicationLag: 'low',
        },
        remediations: [
          'Use a two-phase rename: add the new column, dual-write or copy data, deploy clients to read the new name, then drop the old column later.',
        ],
      }),
    );
    return;
  }

  if (/^rename\s+to\b/i.test(normalized)) {
    findings.push(
      finding(statement, {
        id: 'rename-table',
        title: `Renaming ${table} is a client schema break`,
        severity: 'warning',
        details:
          'The old table name disappears from the published schema. Active clients whose Zero schema still includes the old table can get SchemaVersionNotSupported.',
        impact: {
          backfill: 'no',
          schemaVersionNotSupported: 'possible',
          replicationLag: 'low',
        },
        remediations: [
          'Use a two-phase table rename: create the new table, mirror data/writes, deploy clients to the new table, then remove the old table later.',
        ],
      }),
    );
    return;
  }

  const typeChange = parseAlterColumnType(normalized);
  if (typeChange) {
    const zqlType = zqlTypeForPostgresType(typeChange.type);
    findings.push(
      finding(statement, {
        id: zqlType ? 'alter-column-type' : 'alter-column-unsupported-type',
        title: `Changing ${table}.${typeChange.column} type affects Zero schema`,
        severity: 'warning',
        details: zqlType
          ? `The new type maps to Zero ${zqlType}, but active clients can still break if their generated client schema no longer matches the published schema. Large Postgres type rewrites can also create operational lag.`
          : 'The new type is not known to map to a Zero client value type. If a client schema includes this column, it can get SchemaVersionNotSupported.',
        impact: {
          backfill: 'no',
          schemaVersionNotSupported: 'possible',
          replicationLag: 'medium',
        },
        remediations: [
          'For large tables, prefer add-new-column, batch-copy, deploy clients, then drop the old column.',
          'Keep old clients compatible until they no longer declare the previous column type.',
        ],
      }),
    );
    return;
  }

  if (
    /^alter\s+(?:column\s+)?/i.test(normalized) &&
    /\bdrop\s+not\s+null\b/i.test(normalized)
  ) {
    findings.push(
      finding(statement, {
        id: 'drop-not-null',
        title: `Dropping NOT NULL on ${table} can remove a Zero key candidate`,
        severity: 'warning',
        details:
          'Zero requires the client primaryKey to correspond to a primary key or non-null unique index. If this column participates in the key selected by the client schema, clients can get SchemaVersionNotSupported.',
        impact: {
          backfill: 'no',
          schemaVersionNotSupported: 'possible',
          replicationLag: 'low',
        },
        remediations: [
          'Keep NOT NULL on columns used by Zero primary keys, or create a replacement non-null unique key before deploying this change.',
        ],
      }),
    );
    return;
  }
}

function analyzeDropTable(statement: SqlStatement, sql: string): Finding[] {
  if (!/^\s*drop\s+table\b/i.test(sql)) {
    return [];
  }
  return [
    finding(statement, {
      id: 'drop-table',
      title: 'Dropping a table can break active Zero clients',
      severity: 'warning',
      details:
        'If any active client schema includes the dropped table, zero-cache will reject that schema with SchemaVersionNotSupported after the table disappears from the published schema.',
      impact: {
        backfill: 'no',
        schemaVersionNotSupported: 'possible',
        replicationLag: 'low',
      },
      remediations: [
        'Deploy clients that no longer declare or query the table before dropping it.',
        'Wait for old clients/client groups to age out, then drop the table in a later migration.',
      ],
    }),
  ];
}

function analyzePublication(statement: SqlStatement, sql: string): Finding[] {
  const findings: Finding[] = [];
  if (/^\s*alter\s+publication\b/i.test(sql)) {
    if (/\badd\s+table\b/i.test(sql)) {
      findings.push(
        finding(statement, {
          id: 'publication-add-table',
          title:
            'Adding existing tables to a publication triggers Zero backfill',
          severity: 'warning',
          details:
            'Tables introduced into Zero by ALTER PUBLICATION are backfilled because they may already contain rows. This can increase replication lag until the backfill completes.',
          impact: {
            backfill: 'yes',
            schemaVersionNotSupported: 'possible',
            replicationLag: 'high',
          },
          remediations: [
            'Add large tables to the publication during a low-traffic window.',
            'If possible, create and publish the table while it is empty, then load data in batches.',
            'Roll out client code that requires the table only after the backfill has completed.',
          ],
        }),
      );
    }
    if (/\bset\s+table\b/i.test(sql)) {
      findings.push(
        finding(statement, {
          id: 'publication-set-table',
          title: 'SET TABLE can both backfill and remove published schema',
          severity: 'warning',
          details:
            'ALTER PUBLICATION ... SET TABLE can add existing tables, which Zero backfills, and remove tables, which can break clients that still declare them.',
          impact: {
            backfill: 'possible',
            schemaVersionNotSupported: 'possible',
            replicationLag: 'high',
          },
          remediations: [
            'Prefer explicit ADD TABLE and DROP TABLE changes in separate migrations.',
            'Backfill newly added large tables before clients depend on them.',
            'Remove tables from client schemas before removing them from the publication.',
          ],
        }),
      );
    }
    if (/\bdrop\s+table\b/i.test(sql)) {
      findings.push(
        finding(statement, {
          id: 'publication-drop-table',
          title: 'Dropping tables from a publication can break clients',
          severity: 'warning',
          details:
            'The table disappears from Zero replicated schema. Clients that still include the table in their Zero schema can get SchemaVersionNotSupported.',
          impact: {
            backfill: 'no',
            schemaVersionNotSupported: 'possible',
            replicationLag: 'low',
          },
          remediations: [
            'Deploy clients that no longer use the table before removing it from the publication.',
          ],
        }),
      );
    }
    if (/\([^)]+\)/.test(sql) && /\b(?:add|set)\s+table\b/i.test(sql)) {
      findings.push(
        finding(statement, {
          id: 'publication-column-list',
          title: 'Publication column lists can hide columns from Zero',
          severity: 'warning',
          details:
            'Changing a publication column list can add columns that require backfill or remove columns that active clients still declare.',
          impact: {
            backfill: 'possible',
            schemaVersionNotSupported: 'possible',
            replicationLag: 'medium',
          },
          remediations: [
            'Treat publication column additions like ALTER PUBLICATION backfills.',
            'Remove columns from the client schema before excluding them from the publication.',
          ],
        }),
      );
    }
  }

  if (/^\s*comment\s+on\s+publication\b/i.test(sql)) {
    findings.push(
      finding(statement, {
        id: 'publication-comment-snapshot',
        title: 'COMMENT ON PUBLICATION may trigger a Zero schema snapshot',
        severity: 'info',
        details:
          'Zero uses COMMENT ON PUBLICATION as a schema snapshot hook on databases where ALTER PUBLICATION event triggers are unavailable. When it reveals added tables or columns, Zero conservatively treats the command tag as UNKNOWN and backfills them.',
        impact: {
          backfill: 'possible',
          schemaVersionNotSupported: 'possible',
          replicationLag: 'medium',
        },
        remediations: [
          'If this follows ALTER PUBLICATION, expect the same backfill/client-compatibility risks as the publication change.',
        ],
      }),
    );
  }

  if (/^\s*(?:create|drop)\s+publication\b/i.test(sql)) {
    findings.push(
      finding(statement, {
        id: 'publication-recreate',
        title:
          'Creating or dropping a publication changes Zero replication scope',
        severity: 'warning',
        details:
          'Changing the publication itself can add or remove many tables from Zero. Added existing tables can backfill; removed tables can break client schemas.',
        impact: {
          backfill: 'possible',
          schemaVersionNotSupported: 'possible',
          replicationLag: 'high',
        },
        remediations: [
          'Prefer narrow ALTER PUBLICATION changes that can be reviewed table by table.',
          'Coordinate publication scope changes with client schema deployments.',
        ],
      }),
    );
  }
  return findings;
}

function analyzeIndex(statement: SqlStatement, sql: string): Finding[] {
  if (/^\s*drop\s+index\b/i.test(sql)) {
    return [
      finding(statement, {
        id: 'drop-index',
        title: 'Dropping an index can remove a Zero key candidate',
        severity: 'warning',
        details:
          'Dropping an index is usually safe for Zero, but dropping a non-null unique index selected as a client primaryKey can make the table unsyncable for that client schema.',
        impact: {
          backfill: 'no',
          schemaVersionNotSupported: 'possible',
          replicationLag: 'low',
        },
        remediations: [
          'Confirm the Zero schema primaryKey still maps to a primary key or non-null unique index after the migration.',
          'Create a replacement key before dropping the old unique index.',
        ],
      }),
    ];
  }
  return [];
}

function analyzeReplicaIdentity(
  statement: SqlStatement,
  sql: string,
): Finding[] {
  if (!/\breplica\s+identity\b/i.test(sql)) {
    return [];
  }
  const full = /\breplica\s+identity\s+full\b/i.test(sql);
  const nothing = /\breplica\s+identity\s+nothing\b/i.test(sql);
  return [
    finding(statement, {
      id: 'replica-identity-change',
      title: 'Replica identity changes affect logical replication payloads',
      severity: full || nothing ? 'warning' : 'info',
      details: full
        ? 'REPLICA IDENTITY FULL can significantly increase update/delete payload size for wide tables, which can increase replication lag.'
        : nothing
          ? 'REPLICA IDENTITY NOTHING can prevent reliable update/delete replication for tables without another usable identity.'
          : 'Replica identity changes are often safe, but they determine how Zero can identify rows for updates and deletes.',
      impact: {
        backfill: 'no',
        schemaVersionNotSupported: 'possible',
        replicationLag: full ? 'medium' : 'low',
      },
      remediations: [
        'Prefer a primary key or a narrow unique replica identity index over REPLICA IDENTITY FULL for large tables.',
      ],
    }),
  ];
}

function analyzeBroadDml(statement: SqlStatement, sql: string): Finding[] {
  if (/^\s*truncate\b/i.test(sql)) {
    return [
      finding(statement, {
        id: 'truncate',
        title: 'TRUNCATE is replicated as a broad table change',
        severity: 'warning',
        details:
          'TRUNCATE can invalidate many client-visible rows at once. It does not cause a schema backfill, but it can produce a large visible sync update.',
        impact: {
          backfill: 'no',
          schemaVersionNotSupported: 'no',
          replicationLag: 'medium',
        },
        remediations: [
          'Avoid truncating published tables while clients depend on their contents, or schedule it during low traffic.',
        ],
      }),
    ];
  }

  if (/^\s*(?:update|delete\s+from)\b/i.test(sql) && !/\bwhere\b/i.test(sql)) {
    return [
      finding(statement, {
        id: 'unbounded-dml',
        title: 'Unbounded DML can spike replication lag',
        severity: 'warning',
        details:
          'A migration UPDATE or DELETE without a WHERE clause can generate a change for every row in a published table.',
        impact: {
          backfill: 'no',
          schemaVersionNotSupported: 'no',
          replicationLag: 'high',
        },
        remediations: [
          'Batch large data changes by primary key range and let replication catch up between batches.',
          'Avoid mixing large DML with schema changes that also require backfill.',
        ],
      }),
    ];
  }

  if (/^\s*insert\s+into\b/i.test(sql) && /\bselect\b/i.test(sql)) {
    return [
      finding(statement, {
        id: 'insert-select',
        title: 'INSERT ... SELECT can create a large replicated write burst',
        severity: 'info',
        details:
          'Bulk inserts into a published table do not cause a Zero schema backfill, but they can increase replication lag while the rows stream to zero-cache.',
        impact: {
          backfill: 'no',
          schemaVersionNotSupported: 'no',
          replicationLag: 'medium',
        },
        remediations: [
          'Batch large INSERT ... SELECT operations when the destination table is published.',
        ],
      }),
    ];
  }
  return [];
}

function parseCreateTableName(
  sql: string,
): {name: string; end: number} | undefined {
  const match =
    /^\s*create\s+(?:temporary\s+|temp\s+|unlogged\s+)?table\s+(?:if\s+not\s+exists\s+)?/i.exec(
      sql,
    );
  if (!match) {
    return undefined;
  }
  const parsed = readQualifiedIdentifier(sql, match[0].length);
  return parsed ? {name: parsed.display, end: parsed.end} : undefined;
}

function parseAlterTable(
  sql: string,
): {table: string; actions: string} | undefined {
  const match = /^\s*alter\s+table\s+(?:if\s+exists\s+)?(?:only\s+)?/i.exec(
    sql,
  );
  if (!match) {
    return undefined;
  }
  const table = readQualifiedIdentifier(sql, match[0].length);
  if (!table) {
    return undefined;
  }
  return {
    table: table.display,
    actions: sql.slice(table.end).trim(),
  };
}

function parseCreateTableBody(body: string) {
  const columns = new Map<string, ColumnDefinition>();
  const primaryKeys: string[][] = [];
  const uniqueKeys: string[][] = [];

  for (const part of splitTopLevel(body, ',')) {
    if (/^(?:constraint|foreign|check|exclude)\b/i.test(part)) {
      const primaryKey = parseColumnsInParensAfter(part, 'primary key');
      if (primaryKey.length) {
        primaryKeys.push(primaryKey);
      }
      const uniqueKey = parseColumnsInParensAfter(part, 'unique');
      if (uniqueKey.length) {
        uniqueKeys.push(uniqueKey);
      }
      continue;
    }
    if (/^primary\s+key\b/i.test(part)) {
      const primaryKey = parseColumnsInParensAfter(part, 'primary key');
      if (primaryKey.length) {
        primaryKeys.push(primaryKey);
      }
      continue;
    }
    if (/^unique\b/i.test(part)) {
      const uniqueKey = parseColumnsInParensAfter(part, 'unique');
      if (uniqueKey.length) {
        uniqueKeys.push(uniqueKey);
      }
      continue;
    }
    const column = parseColumnDefinition(part);
    if (column) {
      columns.set(column.name, column);
      if (column.primaryKey) {
        primaryKeys.push([column.name]);
      }
      if (column.unique) {
        uniqueKeys.push([column.name]);
      }
    }
  }

  return {columns, primaryKeys, uniqueKeys};
}

function parseColumnDefinition(input: string): ColumnDefinition | undefined {
  const name = readIdentifier(input);
  if (!name) {
    return undefined;
  }
  const restStart = skipWhitespace(input, name.end);
  const rest = input.slice(restStart).trim();
  const firstConstraint = findFirstTopLevelKeyword(
    rest,
    TYPE_CONSTRAINT_KEYWORDS,
  );
  const type = (
    firstConstraint ? rest.slice(0, firstConstraint.index) : rest
  ).trim();
  const constraints = firstConstraint ? rest.slice(firstConstraint.index) : '';
  if (!type) {
    return undefined;
  }
  return {
    name: name.display,
    type,
    rest: constraints,
    defaultExpression: extractDefaultExpression(constraints),
    notNull:
      /\bnot\s+null\b/i.test(constraints) ||
      /\bprimary\s+key\b/i.test(constraints),
    primaryKey: /\bprimary\s+key\b/i.test(constraints),
    unique: /\bunique\b/i.test(constraints),
  };
}

function parseAlterColumnType(
  action: string,
): {column: string; type: string} | undefined {
  const match = /^alter\s+(?:column\s+)?/i.exec(action);
  if (!match) {
    return undefined;
  }
  const column = readIdentifier(action, match[0].length);
  if (!column) {
    return undefined;
  }
  const afterColumn = action.slice(column.end).trim();
  const typeMatch = /^(?:set\s+data\s+)?type\s+/i.exec(afterColumn);
  if (!typeMatch) {
    return undefined;
  }
  const rawType = afterColumn.slice(typeMatch[0].length).trim();
  const usingIndex = findTopLevelKeyword(rawType, 'using');
  return {
    column: column.display,
    type: (usingIndex === -1 ? rawType : rawType.slice(0, usingIndex)).trim(),
  };
}

function extractParenthesized(
  input: string,
  start: number,
): string | undefined {
  const open = input.indexOf('(', start);
  if (open === -1) {
    return undefined;
  }
  let depth = 0;
  for (let i = open; i < input.length; i++) {
    const ch = input[i];
    if (ch === '(') {
      depth++;
    } else if (ch === ')') {
      depth--;
      if (depth === 0) {
        return input.slice(open + 1, i);
      }
    }
  }
  return undefined;
}

function parseColumnsInParensAfter(input: string, keyword: string): string[] {
  const keywordIndex = findTopLevelKeyword(input, keyword);
  if (keywordIndex === -1) {
    return [];
  }
  const body = extractParenthesized(input, keywordIndex + keyword.length);
  if (!body) {
    return [];
  }
  return splitTopLevel(body, ',')
    .map(part => readIdentifier(part)?.display)
    .filter((part): part is string => part !== undefined);
}

function extractDefaultExpression(constraints: string): string | null {
  const index = findTopLevelKeyword(constraints, 'default');
  if (index === -1) {
    return null;
  }
  const start = skipWhitespace(constraints, index + 'default'.length);
  const next = findFirstTopLevelKeyword(
    constraints,
    DEFAULT_TERMINATORS,
    start,
  );
  return (
    next ? constraints.slice(start, next.index) : constraints.slice(start)
  )
    .trim()
    .replace(/,$/, '')
    .trim();
}

function potentialPrimaryKeys(parsed: {
  columns: ReadonlyMap<string, ColumnDefinition>;
  primaryKeys: readonly string[][];
  uniqueKeys: readonly string[][];
}) {
  const nonNullUnique = parsed.uniqueKeys.filter(key =>
    key.every(column => parsed.columns.get(column)?.notNull),
  );
  return [...parsed.primaryKeys, ...nonNullUnique];
}

function addUnsupportedColumnFindings(
  findings: Finding[],
  statement: SqlStatement,
  columns: ReadonlyMap<string, ColumnDefinition>,
  table: string,
) {
  for (const column of columns.values()) {
    if (zqlTypeForPostgresType(column.type) !== undefined) {
      continue;
    }
    findings.push(
      finding(statement, {
        id: 'unsupported-column-type',
        title: `${table}.${column.name} uses a type Zero may not sync`,
        severity: 'warning',
        details: `The column type "${column.type}" is not in Zero's built-in PostgreSQL type map. If this is not an enum or supported array, clients that declare the column can get SchemaVersionNotSupported.`,
        impact: {
          backfill: 'no',
          schemaVersionNotSupported: 'possible',
          replicationLag: 'low',
        },
        remediations: [
          'Use a Zero-supported type, or keep the column out of the Zero client schema.',
          'For enums, confirm Postgres reports the type as an enum so Zero maps it to string.',
        ],
      }),
    );
  }
}

function addColumnDefaultFinding(
  findings: Finding[],
  statement: SqlStatement,
  table: string,
  column: ColumnDefinition,
  zeroVersion: ZeroVersionProfile,
) {
  if (!zeroVersion.supportsAddColumnDefaults) {
    findings.push(
      finding(statement, {
        id: 'add-column-default-unsupported-by-zero-version',
        title: `Adding ${table}.${column.name} with a default is unsafe for Zero ${zeroVersion.label}`,
        severity: 'error',
        details:
          `Zero ${zeroVersion.label} predates restored ADD COLUMN default handling. ` +
          `The default expression ${column.defaultExpression} may not be represented correctly in the replica.`,
        impact: {
          backfill: 'no',
          schemaVersionNotSupported: 'possible',
          replicationLag: 'low',
        },
        remediations: [
          'Upgrade zero-cache before running this migration.',
          'Or add the column nullable with no default, backfill application data in batches, then add the default and NOT NULL in a later migration.',
        ],
      }),
    );
    return;
  }

  if (!defaultRequiresBackfill(column)) {
    return;
  }

  if (!zeroVersion.autoBackfillsUnsupportedDefaults) {
    findings.push(
      finding(statement, {
        id: 'add-column-default-unsupported-by-zero-version',
        title: `Adding ${table}.${column.name} with this default is unsupported by Zero ${zeroVersion.label}`,
        severity: 'error',
        details: `The default expression ${column.defaultExpression} is not one of the simple defaults Zero can apply with SQLite ADD COLUMN, and Zero ${zeroVersion.label} predates auto-backfill for unsupported ADD COLUMN defaults.`,
        impact: {
          backfill: 'no',
          schemaVersionNotSupported: 'possible',
          replicationLag: 'low',
        },
        remediations: [
          'Upgrade to Zero 0.26.0 or newer before running this migration.',
          'Or use a two-phase migration: add the column nullable with no default, populate it in batches, then add the default or constraint later.',
        ],
      }),
    );
    return;
  }

  findings.push(
    finding(statement, {
      id: 'add-column-backfill',
      title: `Adding ${table}.${column.name} will require a Zero backfill`,
      severity: 'warning',
      details:
        `The default expression ${column.defaultExpression} is not one of the simple defaults Zero can apply with SQLite ADD COLUMN. ` +
        'Zero will hide the column and backfill values from Postgres before publishing it to clients.',
      impact: {
        backfill: 'yes',
        schemaVersionNotSupported: 'possible',
        replicationLag: 'high',
      },
      remediations: [
        'Add the column as nullable with no default, or with a simple constant default Zero can map directly.',
        'Populate existing rows in small batches, then add the default or NOT NULL constraint in a later migration.',
        'Schedule the migration during low write volume if the table is large.',
      ],
    }),
  );
}

function defaultRequiresBackfill(column: ColumnDefinition): boolean {
  const dflt = column.defaultExpression?.trim();
  if (!dflt || /^null$/i.test(dflt)) {
    return false;
  }
  if (/^-?\d+(?:\.\d+)?$/.test(dflt)) {
    return false;
  }
  if (/^(?:true|false)$/i.test(dflt)) {
    return false;
  }
  if (/^'.*'(?:::[A-Za-z_][A-Za-z0-9_]*)?$/s.test(dflt)) {
    return false;
  }
  if (/^array\s*\[\s*\]::[A-Za-z_][A-Za-z0-9_]*\[\]$/i.test(dflt)) {
    return false;
  }
  if (/^'\{\}'::[A-Za-z_][A-Za-z0-9_]*\[\]$/i.test(dflt)) {
    return false;
  }
  return true;
}

function isLikelyForeignKeyConstraint(name: string): boolean {
  return /(?:^|_)fkey$/i.test(name) || /(?:^|_)fk(?:_|$)/i.test(name);
}

function zqlTypeForPostgresType(type: string): string | undefined {
  const normalized = normalizePostgresType(type);
  if (normalized.endsWith('[]')) {
    return 'json';
  }
  return SUPPORTED_TYPES.get(normalized);
}

function normalizePostgresType(type: string): string {
  return type
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/\s*\([^)]*\)/g, '')
    .replace(/^public\./i, '')
    .toLowerCase();
}

function summarize(findings: readonly Finding[]): ImpactSummary {
  const maxSeverity = maxByRank(
    findings.map(finding => finding.severity),
    {info: 0, warning: 1, error: 2},
  );
  return {
    safety:
      maxSeverity === 'error'
        ? 'unsafe'
        : maxSeverity === 'warning'
          ? 'review'
          : 'safe',
    backfill: maxImpact(findings.map(finding => finding.impact.backfill)),
    schemaVersionNotSupported: maxImpact(
      findings.map(finding => finding.impact.schemaVersionNotSupported),
    ),
    replicationLag: maxByRank(
      findings.map(finding => finding.impact.replicationLag),
      {low: 0, medium: 1, high: 2},
    ),
  };
}

function maxImpact(values: readonly ImpactAnswer[]): ImpactAnswer {
  return maxByRank(values, {no: 0, possible: 1, yes: 2});
}

function maxByRank<T extends string>(
  values: readonly T[],
  ranks: Record<T, number>,
): T {
  let best = Object.keys(ranks)[0] as T;
  for (const value of values) {
    if (ranks[value] > ranks[best]) {
      best = value;
    }
  }
  return best;
}

function finding(
  statement: SqlStatement,
  details: Omit<Finding, 'location' | 'statement'>,
): Finding {
  return {
    ...details,
    location: {
      file: statement.file,
      startLine: statement.startLine,
      endLine: statement.endLine,
      statementIndex: statement.index,
    },
    statement: summarizeStatement(statement.sql),
  };
}

function summarizeStatement(sql: string): string {
  const normalized = normalizeSql(sql);
  return normalized.length > 220
    ? normalized.slice(0, 217).trimEnd() + '...'
    : normalized;
}
