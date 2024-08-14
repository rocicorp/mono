export function liteTableName(t: {schema: string; name: string}) {
  return t.schema === 'public' ? t.name : `${t.schema}.${t.name}`;
}
