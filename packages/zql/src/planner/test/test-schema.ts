import {relationships} from '../../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import {createBuilder} from '../../query/create-builder.ts';

// Minimal test schema for planner tests
const users = table('users')
  .columns({
    id: number(),
    active: boolean().optional(),
    admin: boolean().optional(),
    status: string().optional(),
  })
  .primaryKey('id');

const posts = table('posts')
  .columns({
    id: number(),
    userId: number(),
    title: string().optional(),
  })
  .primaryKey('id');

const comments = table('comments')
  .columns({
    id: number(),
    userId: number(),
    postId: number().optional(),
  })
  .primaryKey('id');

const profile = table('profile')
  .columns({
    id: number(),
    userId: number(),
  })
  .primaryKey('id');

const likes = table('likes')
  .columns({
    id: number(),
    authorId: number(),
  })
  .primaryKey('id');

// Relationships
const usersRelationships = relationships(users, ({many}) => ({
  posts: many({
    sourceField: ['id'],
    destField: ['userId'],
    destSchema: posts,
  }),
  comments: many({
    sourceField: ['id'],
    destField: ['userId'],
    destSchema: comments,
  }),
  profile: many({
    sourceField: ['id'],
    destField: ['userId'],
    destSchema: profile,
  }),
  likes: many({
    sourceField: ['id'],
    destField: ['authorId'],
    destSchema: likes,
  }),
}));

const postsRelationships = relationships(posts, ({one, many}) => ({
  user: one({
    sourceField: ['userId'],
    destField: ['id'],
    destSchema: users,
  }),
  comments: many({
    sourceField: ['id'],
    destField: ['postId'],
    destSchema: comments,
  }),
}));

const commentsRelationships = relationships(comments, ({one}) => ({
  user: one({
    sourceField: ['userId'],
    destField: ['id'],
    destSchema: users,
  }),
  post: one({
    sourceField: ['postId'],
    destField: ['id'],
    destSchema: posts,
  }),
}));

const profileRelationships = relationships(profile, ({one}) => ({
  user: one({
    sourceField: ['userId'],
    destField: ['id'],
    destSchema: users,
  }),
}));

const likesRelationships = relationships(likes, ({one}) => ({
  author: one({
    sourceField: ['authorId'],
    destField: ['id'],
    destSchema: users,
  }),
}));

export const testSchema = createSchema({
  tables: [users, posts, comments, profile, likes],
  relationships: [
    usersRelationships,
    postsRelationships,
    commentsRelationships,
    profileRelationships,
    likesRelationships,
  ],
});

export const builder = createBuilder(testSchema);
