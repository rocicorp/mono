// This file defines a complex query to stress test TypeScript's type
// inference and the Zero query builder: 20 levels of deeply nested .related()
// calls that traverse across multiple business domains to test deep relationship chains
//
// We also export the query and Zero instance so that tsc will try to compile it
// and fail if it can't output .d.ts

import {zeroStress} from './zero-stress-client-test.ts';

const queryDeep = zeroStress.query.order.related('createdByUser', q =>
  q.related('workspaceMembers', q =>
    q.related('workspace', q =>
      q.related('budgets', q =>
        q.related('department', q =>
          q.related('parentDepartment', q =>
            q.related('headOfDepartment', q =>
              q.related('manager', q =>
                q.related('workspace', q =>
                  q.related('agentAssignments', q =>
                    q.related('ticket', q =>
                      q.related('team', q =>
                        q.related('leader', q =>
                          q.related('updatedCmsArticles', q =>
                            q.related('author', q =>
                              q.related('ownedProjects', q =>
                                q.related('owner', q =>
                                  q.related('updatedEntityComments', q =>
                                    q.related('parentComment', q =>
                                      q.related('createdByUser'),
                                    ),
                                  ),
                                ),
                              ),
                            ),
                          ),
                        ),
                      ),
                    ),
                  ),
                ),
              ),
            ),
          ),
        ),
      ),
    ),
  ),
);

// this is testing .d.ts generation for complex queries
export {queryDeep};
