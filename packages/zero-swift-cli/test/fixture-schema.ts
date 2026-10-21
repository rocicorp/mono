/**
 * A schema that exercises the corners of `zero-swift codegen`: keyword and
 * reserved names, enumerations shared between columns, nested JSON types,
 * integer overrides, compound keys, and two-hop relationships.
 */
import {relationships} from '../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  enumeration,
  json,
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';

type Status = 'open' | 'in-progress' | 'closed';

type Settings = {
  theme: 'light' | 'dark';
  fontSize: number;
  tags: string[];
  shortcuts?: {key: string; command: string}[] | undefined;
  extra: string | number | null;
};

const project = table('project')
  .from('projects')
  .columns({
    id: string(),
    name: string(),
    default: boolean(),
    status: enumeration<Status>(),
    settings: json<Settings>(),
    position: number(),
    archivedStatus: enumeration<Status>().optional(),
  })
  .primaryKey('id');

const query = table('query')
  .columns({
    id: string(),
    projectID: string().from('project_id'),
    text: string(),
    class: string().optional(),
  })
  .primaryKey('id');

const member = table('member')
  .columns({
    projectID: string(),
    userID: string(),
    role: enumeration<'owner' | 'editor' | 'viewer'>(),
  })
  .primaryKey('projectID', 'userID');

const person = table('person')
  .columns({
    id: string(),
    name: string(),
    nickname: string().optional(),
  })
  .primaryKey('id')
  .unique('name');

const projectRelationships = relationships(project, ({many}) => ({
  queries: many({
    sourceField: ['id'],
    destField: ['projectID'],
    destSchema: query,
  }),
  people: many(
    {sourceField: ['id'], destField: ['projectID'], destSchema: member},
    {sourceField: ['userID'], destField: ['id'], destSchema: person},
  ),
}));

const queryRelationships = relationships(query, ({one}) => ({
  project: one({
    sourceField: ['projectID'],
    destField: ['id'],
    destSchema: project,
  }),
}));

export const schema = createSchema({
  tables: [project, query, member, person],
  relationships: [projectRelationships, queryRelationships],
});
