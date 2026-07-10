import {ANYONE_CAN, definePermissions} from '@rocicorp/zero';
import {schema} from './schema.ts';

export {schema};

export const permissions = await definePermissions(schema, () => ({
  event: {
    row: {
      select: ANYONE_CAN,
    },
  },
  emailThread: {
    row: {
      select: ANYONE_CAN,
    },
  },
  emailMessage: {
    row: {
      select: ANYONE_CAN,
    },
  },
  forumUser: {
    row: {
      select: ANYONE_CAN,
    },
  },
  forumCategory: {
    row: {
      select: ANYONE_CAN,
    },
  },
  forumThread: {
    row: {
      select: ANYONE_CAN,
    },
  },
  forumPost: {
    row: {
      select: ANYONE_CAN,
    },
  },
  relOrg: {
    row: {
      select: ANYONE_CAN,
    },
  },
  relAccount: {
    row: {
      select: ANYONE_CAN,
    },
  },
  relContact: {
    row: {
      select: ANYONE_CAN,
    },
  },
  relActivity: {
    row: {
      select: ANYONE_CAN,
    },
  },
}));
