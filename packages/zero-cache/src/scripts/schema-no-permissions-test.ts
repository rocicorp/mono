import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';

const member = table('member')
  .columns({
    id: string(),
    name: string(),
    parentID: string().optional(),
  })
  .primaryKey('id');

const memberRelationships = relationships(member, ({one}) => ({
  parent: one({
    sourceField: ['parentID'],
    destField: ['id'],
    destSchema: member,
  }),
}));

export const schema = createSchema({
  tables: [member],
  relationships: [memberRelationships],
});
