import {ANYONE_CAN, definePermissions} from '@rocicorp/zero';
import {schema} from './schema.ts';

export {schema};

export const permissions = await definePermissions(schema, () => ({
  event: {
    row: {
      select: ANYONE_CAN,
    },
  },
}));
