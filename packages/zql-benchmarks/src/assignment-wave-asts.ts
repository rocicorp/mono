/**
 * The exact post-auth transformed ASTs the zero-cache planning diagnosis
 * captured from the educator assignment-detail wave, verbatim from the
 * `Slow query materialization` log lines of its eight-group run.
 *
 * These are the planner's real input: permissions are already expanded and
 * auth parameters are already bound to literals, but ordering completion and
 * scalar resolution have not run yet. Every identity in them is synthetic.
 */
import type {AST} from '../../zero-protocol/src/ast.ts';

export const ROSTER_AST: AST = {
  table: 'assignment',
  where: {
    type: 'and',
    conditions: [
      {
        type: 'simple',
        left: {
          type: 'column',
          name: 'id',
        },
        right: {
          type: 'literal',
          value: 'assignment_emu_lag_136',
        },
        op: '=',
      },
      {
        type: 'or',
        conditions: [
          {
            type: 'correlatedSubquery',
            related: {
              system: 'client',
              correlation: {
                parentField: ['id'],
                childField: ['assignment_id'],
              },
              subquery: {
                table: 'teacher_assignment_access',
                alias: 'zsubq_teacher_access',
                where: {
                  type: 'correlatedSubquery',
                  related: {
                    system: 'client',
                    correlation: {
                      parentField: ['teacher_id'],
                      childField: ['id'],
                    },
                    subquery: {
                      table: 'teacher',
                      alias: 'zsubq_teacher',
                      where: {
                        type: 'simple',
                        left: {
                          type: 'column',
                          name: 'user_id',
                        },
                        right: {
                          type: 'literal',
                          value: 'user_emu_lag_teacher',
                        },
                        op: '=',
                      },
                    },
                  },
                  op: 'EXISTS',
                  flip: true,
                },
              },
            },
            op: 'EXISTS',
          },
          {
            type: 'correlatedSubquery',
            related: {
              system: 'client',
              correlation: {
                parentField: ['assignment_group_id'],
                childField: ['id'],
              },
              subquery: {
                table: 'assignment_group',
                alias: 'zsubq_assignment_group',
                where: {
                  type: 'correlatedSubquery',
                  related: {
                    system: 'client',
                    correlation: {
                      parentField: ['created_by_teacher_id'],
                      childField: ['id'],
                    },
                    subquery: {
                      table: 'teacher',
                      alias: 'zsubq_created_by',
                      where: {
                        type: 'correlatedSubquery',
                        related: {
                          system: 'client',
                          correlation: {
                            parentField: ['school_id'],
                            childField: ['id'],
                          },
                          subquery: {
                            table: 'school',
                            alias: 'zsubq_school',
                            where: {
                              type: 'correlatedSubquery',
                              related: {
                                system: 'client',
                                correlation: {
                                  parentField: ['group_id'],
                                  childField: ['id'],
                                },
                                subquery: {
                                  table: 'school_group',
                                  alias: 'zsubq_group',
                                  where: {
                                    type: 'correlatedSubquery',
                                    related: {
                                      system: 'client',
                                      correlation: {
                                        parentField: ['id'],
                                        childField: ['group_id'],
                                      },
                                      subquery: {
                                        table: 'school',
                                        alias: 'zsubq_schools',
                                        where: {
                                          type: 'correlatedSubquery',
                                          related: {
                                            system: 'client',
                                            correlation: {
                                              parentField: ['id'],
                                              childField: ['school_id'],
                                            },
                                            subquery: {
                                              table: 'teacher',
                                              alias: 'zsubq_teachers',
                                              where: {
                                                type: 'and',
                                                conditions: [
                                                  {
                                                    type: 'simple',
                                                    left: {
                                                      type: 'column',
                                                      name: 'user_id',
                                                    },
                                                    right: {
                                                      type: 'literal',
                                                      value:
                                                        'user_emu_lag_teacher',
                                                    },
                                                    op: '=',
                                                  },
                                                  {
                                                    type: 'simple',
                                                    left: {
                                                      type: 'column',
                                                      name: 'role',
                                                    },
                                                    right: {
                                                      type: 'literal',
                                                      value: 'administrator',
                                                    },
                                                    op: '=',
                                                  },
                                                ],
                                              },
                                            },
                                          },
                                          op: 'EXISTS',
                                          flip: true,
                                        },
                                      },
                                    },
                                    op: 'EXISTS',
                                    flip: true,
                                  },
                                },
                              },
                              op: 'EXISTS',
                              flip: true,
                            },
                          },
                        },
                        op: 'EXISTS',
                        flip: true,
                      },
                    },
                  },
                  op: 'EXISTS',
                  flip: true,
                },
              },
            },
            op: 'EXISTS',
            flip: true,
          },
          {
            type: 'and',
            conditions: [
              {
                type: 'simple',
                left: {
                  type: 'column',
                  name: 'creation_reason',
                },
                right: {
                  type: 'literal',
                  value: 'instant-practice',
                },
                op: '=',
              },
              {
                type: 'correlatedSubquery',
                related: {
                  system: 'client',
                  correlation: {
                    parentField: ['id'],
                    childField: ['assignment_id'],
                  },
                  subquery: {
                    table: 'assignment_to_student',
                    alias: 'zsubq_students',
                    where: {
                      type: 'correlatedSubquery',
                      related: {
                        system: 'client',
                        correlation: {
                          parentField: ['student_id'],
                          childField: ['id'],
                        },
                        subquery: {
                          table: 'student',
                          alias: 'zsubq_student',
                          where: {
                            type: 'correlatedSubquery',
                            related: {
                              system: 'client',
                              correlation: {
                                parentField: ['id'],
                                childField: ['student_id'],
                              },
                              subquery: {
                                table: 'student_class_membership',
                                alias: 'zsubq_classes',
                                where: {
                                  type: 'correlatedSubquery',
                                  related: {
                                    system: 'client',
                                    correlation: {
                                      parentField: ['class_id'],
                                      childField: ['id'],
                                    },
                                    subquery: {
                                      table: 'class',
                                      alias: 'zsubq_class',
                                      where: {
                                        type: 'correlatedSubquery',
                                        related: {
                                          system: 'client',
                                          correlation: {
                                            parentField: ['id'],
                                            childField: ['class_id'],
                                          },
                                          subquery: {
                                            table: 'teacher_class_access',
                                            alias: 'zsubq_teacher_access',
                                            where: {
                                              type: 'correlatedSubquery',
                                              related: {
                                                system: 'client',
                                                correlation: {
                                                  parentField: ['teacher_id'],
                                                  childField: ['id'],
                                                },
                                                subquery: {
                                                  table: 'teacher',
                                                  alias: 'zsubq_teacher',
                                                  where: {
                                                    type: 'simple',
                                                    left: {
                                                      type: 'column',
                                                      name: 'user_id',
                                                    },
                                                    right: {
                                                      type: 'literal',
                                                      value:
                                                        'user_emu_lag_teacher',
                                                    },
                                                    op: '=',
                                                  },
                                                },
                                              },
                                              op: 'EXISTS',
                                              flip: true,
                                            },
                                          },
                                        },
                                        op: 'EXISTS',
                                        flip: true,
                                      },
                                    },
                                  },
                                  op: 'EXISTS',
                                },
                              },
                            },
                            op: 'EXISTS',
                          },
                        },
                      },
                      op: 'EXISTS',
                    },
                  },
                },
                op: 'EXISTS',
              },
            ],
          },
        ],
      },
    ],
  },
  related: [
    {
      correlation: {
        parentField: ['id'],
        childField: ['assignment_id'],
      },
      subquery: {
        table: 'assignment_to_class',
        alias: 'classes',
        where: {
          type: 'correlatedSubquery',
          related: {
            system: 'client',
            correlation: {
              parentField: ['class_id'],
              childField: ['id'],
            },
            subquery: {
              table: 'class',
              alias: 'zsubq_class',
              where: {
                type: 'correlatedSubquery',
                related: {
                  system: 'client',
                  correlation: {
                    parentField: ['id'],
                    childField: ['class_id'],
                  },
                  subquery: {
                    table: 'teacher_class_access',
                    alias: 'zsubq_teacher_access',
                    where: {
                      type: 'correlatedSubquery',
                      related: {
                        system: 'client',
                        correlation: {
                          parentField: ['teacher_id'],
                          childField: ['id'],
                        },
                        subquery: {
                          table: 'teacher',
                          alias: 'zsubq_teacher',
                          where: {
                            type: 'simple',
                            left: {
                              type: 'column',
                              name: 'user_id',
                            },
                            right: {
                              type: 'literal',
                              value: 'user_emu_lag_teacher',
                            },
                            op: '=',
                          },
                        },
                      },
                      op: 'EXISTS',
                      flip: true,
                    },
                  },
                },
                op: 'EXISTS',
                flip: true,
              },
            },
          },
          op: 'EXISTS',
        },
        related: [
          {
            correlation: {
              parentField: ['class_id'],
              childField: ['id'],
            },
            subquery: {
              table: 'class',
              alias: 'class',
              where: {
                type: 'simple',
                left: {
                  type: 'column',
                  name: 'status',
                },
                right: {
                  type: 'literal',
                  value: ['visible'],
                },
                op: 'IN',
              },
              related: [
                {
                  correlation: {
                    parentField: ['id'],
                    childField: ['class_id'],
                  },
                  subquery: {
                    table: 'student_class_membership',
                    alias: 'students',
                    where: {
                      type: 'correlatedSubquery',
                      related: {
                        system: 'client',
                        correlation: {
                          parentField: ['student_id'],
                          childField: ['id'],
                        },
                        subquery: {
                          table: 'student',
                          alias: 'zsubq_student',
                          where: {
                            type: 'correlatedSubquery',
                            related: {
                              system: 'client',
                              correlation: {
                                parentField: ['id'],
                                childField: ['student_id'],
                              },
                              subquery: {
                                table: 'student_class_membership',
                                alias: 'zsubq_classes',
                                where: {
                                  type: 'correlatedSubquery',
                                  related: {
                                    system: 'client',
                                    correlation: {
                                      parentField: ['class_id'],
                                      childField: ['id'],
                                    },
                                    subquery: {
                                      table: 'class',
                                      alias: 'zsubq_class',
                                      where: {
                                        type: 'correlatedSubquery',
                                        related: {
                                          system: 'client',
                                          correlation: {
                                            parentField: ['id'],
                                            childField: ['class_id'],
                                          },
                                          subquery: {
                                            table: 'teacher_class_access',
                                            alias: 'zsubq_teacher_access',
                                            where: {
                                              type: 'correlatedSubquery',
                                              related: {
                                                system: 'client',
                                                correlation: {
                                                  parentField: ['teacher_id'],
                                                  childField: ['id'],
                                                },
                                                subquery: {
                                                  table: 'teacher',
                                                  alias: 'zsubq_teacher',
                                                  where: {
                                                    type: 'simple',
                                                    left: {
                                                      type: 'column',
                                                      name: 'user_id',
                                                    },
                                                    right: {
                                                      type: 'literal',
                                                      value:
                                                        'user_emu_lag_teacher',
                                                    },
                                                    op: '=',
                                                  },
                                                },
                                              },
                                              op: 'EXISTS',
                                              flip: true,
                                            },
                                          },
                                        },
                                        op: 'EXISTS',
                                        flip: true,
                                      },
                                    },
                                  },
                                  op: 'EXISTS',
                                  flip: true,
                                },
                              },
                            },
                            op: 'EXISTS',
                            flip: true,
                          },
                        },
                      },
                      op: 'EXISTS',
                    },
                    related: [
                      {
                        correlation: {
                          parentField: ['student_id'],
                          childField: ['id'],
                        },
                        subquery: {
                          table: 'student',
                          alias: 'student',
                          related: [
                            {
                              correlation: {
                                parentField: ['user_id'],
                                childField: ['id'],
                              },
                              subquery: {
                                table: 'user',
                                alias: 'user',
                              },
                              system: 'client',
                            },
                          ],
                        },
                        system: 'client',
                      },
                    ],
                  },
                  system: 'client',
                },
              ],
            },
            system: 'client',
          },
        ],
      },
      system: 'client',
    },
    {
      correlation: {
        parentField: ['id'],
        childField: ['assignment_id'],
      },
      subquery: {
        table: 'assignment_to_group',
        alias: 'groups',
        where: {
          type: 'correlatedSubquery',
          related: {
            system: 'client',
            correlation: {
              parentField: ['group_id'],
              childField: ['id'],
            },
            subquery: {
              table: 'group',
              alias: 'zsubq_group',
              where: {
                type: 'or',
                conditions: [
                  {
                    type: 'correlatedSubquery',
                    related: {
                      system: 'client',
                      correlation: {
                        parentField: ['teacher_id'],
                        childField: ['id'],
                      },
                      subquery: {
                        table: 'teacher',
                        alias: 'zsubq_teacher',
                        where: {
                          type: 'simple',
                          left: {
                            type: 'column',
                            name: 'user_id',
                          },
                          right: {
                            type: 'literal',
                            value: 'user_emu_lag_teacher',
                          },
                          op: '=',
                        },
                      },
                    },
                    op: 'EXISTS',
                    flip: true,
                  },
                  {
                    type: 'correlatedSubquery',
                    related: {
                      system: 'client',
                      correlation: {
                        parentField: ['teacher_id'],
                        childField: ['id'],
                      },
                      subquery: {
                        table: 'teacher',
                        alias: 'zsubq_teacher',
                        where: {
                          type: 'correlatedSubquery',
                          related: {
                            system: 'client',
                            correlation: {
                              parentField: ['id'],
                              childField: ['teacher_id'],
                            },
                            subquery: {
                              table: 'teacher_to_co_teacher',
                              alias: 'zsubq_co_teacher_grants',
                              where: {
                                type: 'correlatedSubquery',
                                related: {
                                  system: 'client',
                                  correlation: {
                                    parentField: ['co_teacher_id'],
                                    childField: ['id'],
                                  },
                                  subquery: {
                                    table: 'teacher',
                                    alias: 'zsubq_to_teacher',
                                    where: {
                                      type: 'simple',
                                      left: {
                                        type: 'column',
                                        name: 'user_id',
                                      },
                                      right: {
                                        type: 'literal',
                                        value: 'user_emu_lag_teacher',
                                      },
                                      op: '=',
                                    },
                                  },
                                },
                                op: 'EXISTS',
                                flip: true,
                              },
                            },
                          },
                          op: 'EXISTS',
                          flip: true,
                        },
                      },
                    },
                    op: 'EXISTS',
                    flip: true,
                  },
                  {
                    type: 'correlatedSubquery',
                    related: {
                      system: 'client',
                      correlation: {
                        parentField: ['teacher_id'],
                        childField: ['id'],
                      },
                      subquery: {
                        table: 'teacher',
                        alias: 'zsubq_teacher',
                        where: {
                          type: 'correlatedSubquery',
                          related: {
                            system: 'client',
                            correlation: {
                              parentField: ['school_id'],
                              childField: ['id'],
                            },
                            subquery: {
                              table: 'school',
                              alias: 'zsubq_school',
                              where: {
                                type: 'correlatedSubquery',
                                related: {
                                  system: 'client',
                                  correlation: {
                                    parentField: ['id'],
                                    childField: ['school_id'],
                                  },
                                  subquery: {
                                    table: 'teacher',
                                    alias: 'zsubq_teachers',
                                    where: {
                                      type: 'and',
                                      conditions: [
                                        {
                                          type: 'simple',
                                          left: {
                                            type: 'column',
                                            name: 'user_id',
                                          },
                                          right: {
                                            type: 'literal',
                                            value: 'user_emu_lag_teacher',
                                          },
                                          op: '=',
                                        },
                                        {
                                          type: 'simple',
                                          left: {
                                            type: 'column',
                                            name: 'role',
                                          },
                                          right: {
                                            type: 'literal',
                                            value: 'school-administrator',
                                          },
                                          op: '=',
                                        },
                                      ],
                                    },
                                  },
                                },
                                op: 'EXISTS',
                                flip: true,
                              },
                            },
                          },
                          op: 'EXISTS',
                          flip: true,
                        },
                      },
                    },
                    op: 'EXISTS',
                    flip: true,
                  },
                  {
                    type: 'correlatedSubquery',
                    related: {
                      system: 'client',
                      correlation: {
                        parentField: ['teacher_id'],
                        childField: ['id'],
                      },
                      subquery: {
                        table: 'teacher',
                        alias: 'zsubq_teacher',
                        where: {
                          type: 'correlatedSubquery',
                          related: {
                            system: 'client',
                            correlation: {
                              parentField: ['school_id'],
                              childField: ['id'],
                            },
                            subquery: {
                              table: 'school',
                              alias: 'zsubq_school',
                              where: {
                                type: 'correlatedSubquery',
                                related: {
                                  system: 'client',
                                  correlation: {
                                    parentField: ['group_id'],
                                    childField: ['id'],
                                  },
                                  subquery: {
                                    table: 'school_group',
                                    alias: 'zsubq_group',
                                    where: {
                                      type: 'correlatedSubquery',
                                      related: {
                                        system: 'client',
                                        correlation: {
                                          parentField: ['id'],
                                          childField: ['group_id'],
                                        },
                                        subquery: {
                                          table: 'school',
                                          alias: 'zsubq_schools',
                                          where: {
                                            type: 'correlatedSubquery',
                                            related: {
                                              system: 'client',
                                              correlation: {
                                                parentField: ['id'],
                                                childField: ['school_id'],
                                              },
                                              subquery: {
                                                table: 'teacher',
                                                alias: 'zsubq_teachers',
                                                where: {
                                                  type: 'and',
                                                  conditions: [
                                                    {
                                                      type: 'simple',
                                                      left: {
                                                        type: 'column',
                                                        name: 'user_id',
                                                      },
                                                      right: {
                                                        type: 'literal',
                                                        value:
                                                          'user_emu_lag_teacher',
                                                      },
                                                      op: '=',
                                                    },
                                                    {
                                                      type: 'simple',
                                                      left: {
                                                        type: 'column',
                                                        name: 'role',
                                                      },
                                                      right: {
                                                        type: 'literal',
                                                        value: 'administrator',
                                                      },
                                                      op: '=',
                                                    },
                                                  ],
                                                },
                                              },
                                            },
                                            op: 'EXISTS',
                                            flip: true,
                                          },
                                        },
                                      },
                                      op: 'EXISTS',
                                      flip: true,
                                    },
                                  },
                                },
                                op: 'EXISTS',
                                flip: true,
                              },
                            },
                          },
                          op: 'EXISTS',
                          flip: true,
                        },
                      },
                    },
                    op: 'EXISTS',
                    flip: true,
                  },
                ],
              },
            },
          },
          op: 'EXISTS',
        },
        related: [
          {
            correlation: {
              parentField: ['group_id'],
              childField: ['id'],
            },
            subquery: {
              table: 'group',
              alias: 'group',
              related: [
                {
                  correlation: {
                    parentField: ['id'],
                    childField: ['group_id'],
                  },
                  subquery: {
                    table: 'group_to_student',
                    alias: 'students',
                    related: [
                      {
                        correlation: {
                          parentField: ['student_id'],
                          childField: ['id'],
                        },
                        subquery: {
                          table: 'student',
                          alias: 'student',
                        },
                        system: 'client',
                      },
                    ],
                  },
                  system: 'client',
                },
              ],
              orderBy: [['name', 'asc']],
            },
            system: 'client',
          },
        ],
      },
      system: 'client',
    },
    {
      correlation: {
        parentField: ['id'],
        childField: ['assignment_id'],
      },
      subquery: {
        table: 'assignment_to_student',
        alias: 'students',
        where: {
          type: 'correlatedSubquery',
          related: {
            system: 'client',
            correlation: {
              parentField: ['student_id'],
              childField: ['id'],
            },
            subquery: {
              table: 'student',
              alias: 'zsubq_student',
              where: {
                type: 'correlatedSubquery',
                related: {
                  system: 'client',
                  correlation: {
                    parentField: ['id'],
                    childField: ['student_id'],
                  },
                  subquery: {
                    table: 'student_class_membership',
                    alias: 'zsubq_classes',
                    where: {
                      type: 'correlatedSubquery',
                      related: {
                        system: 'client',
                        correlation: {
                          parentField: ['class_id'],
                          childField: ['id'],
                        },
                        subquery: {
                          table: 'class',
                          alias: 'zsubq_class',
                          where: {
                            type: 'correlatedSubquery',
                            related: {
                              system: 'client',
                              correlation: {
                                parentField: ['id'],
                                childField: ['class_id'],
                              },
                              subquery: {
                                table: 'teacher_class_access',
                                alias: 'zsubq_teacher_access',
                                where: {
                                  type: 'correlatedSubquery',
                                  related: {
                                    system: 'client',
                                    correlation: {
                                      parentField: ['teacher_id'],
                                      childField: ['id'],
                                    },
                                    subquery: {
                                      table: 'teacher',
                                      alias: 'zsubq_teacher',
                                      where: {
                                        type: 'simple',
                                        left: {
                                          type: 'column',
                                          name: 'user_id',
                                        },
                                        right: {
                                          type: 'literal',
                                          value: 'user_emu_lag_teacher',
                                        },
                                        op: '=',
                                      },
                                    },
                                  },
                                  op: 'EXISTS',
                                  flip: true,
                                },
                              },
                            },
                            op: 'EXISTS',
                            flip: true,
                          },
                        },
                      },
                      op: 'EXISTS',
                      flip: true,
                    },
                  },
                },
                op: 'EXISTS',
                flip: true,
              },
            },
          },
          op: 'EXISTS',
        },
        related: [
          {
            correlation: {
              parentField: ['student_id'],
              childField: ['id'],
            },
            subquery: {
              table: 'student',
              alias: 'student',
              related: [
                {
                  correlation: {
                    parentField: ['user_id'],
                    childField: ['id'],
                  },
                  subquery: {
                    table: 'user',
                    alias: 'user',
                  },
                  system: 'client',
                },
              ],
            },
            system: 'client',
          },
        ],
      },
      system: 'client',
    },
  ],
  limit: 1,
};

export const TRACKERS_AST: AST = {
  table: 'problem_tracker',
  where: {
    type: 'and',
    conditions: [
      {
        type: 'simple',
        left: {
          type: 'column',
          name: 'assignment_id',
        },
        right: {
          type: 'literal',
          value: 'assignment_emu_lag_136',
        },
        op: '=',
      },
      {
        type: 'correlatedSubquery',
        related: {
          system: 'client',
          correlation: {
            parentField: ['assignment_id'],
            childField: ['id'],
          },
          subquery: {
            table: 'assignment',
            alias: 'zsubq_assignment',
            where: {
              type: 'or',
              conditions: [
                {
                  type: 'correlatedSubquery',
                  related: {
                    system: 'client',
                    correlation: {
                      parentField: ['id'],
                      childField: ['assignment_id'],
                    },
                    subquery: {
                      table: 'teacher_assignment_access',
                      alias: 'zsubq_teacher_access',
                      where: {
                        type: 'correlatedSubquery',
                        related: {
                          system: 'client',
                          correlation: {
                            parentField: ['teacher_id'],
                            childField: ['id'],
                          },
                          subquery: {
                            table: 'teacher',
                            alias: 'zsubq_teacher',
                            where: {
                              type: 'simple',
                              left: {
                                type: 'column',
                                name: 'user_id',
                              },
                              right: {
                                type: 'literal',
                                value: 'user_emu_lag_teacher',
                              },
                              op: '=',
                            },
                          },
                        },
                        op: 'EXISTS',
                        flip: true,
                      },
                    },
                  },
                  op: 'EXISTS',
                },
                {
                  type: 'correlatedSubquery',
                  related: {
                    system: 'client',
                    correlation: {
                      parentField: ['assignment_group_id'],
                      childField: ['id'],
                    },
                    subquery: {
                      table: 'assignment_group',
                      alias: 'zsubq_assignment_group',
                      where: {
                        type: 'correlatedSubquery',
                        related: {
                          system: 'client',
                          correlation: {
                            parentField: ['created_by_teacher_id'],
                            childField: ['id'],
                          },
                          subquery: {
                            table: 'teacher',
                            alias: 'zsubq_created_by',
                            where: {
                              type: 'correlatedSubquery',
                              related: {
                                system: 'client',
                                correlation: {
                                  parentField: ['school_id'],
                                  childField: ['id'],
                                },
                                subquery: {
                                  table: 'school',
                                  alias: 'zsubq_school',
                                  where: {
                                    type: 'correlatedSubquery',
                                    related: {
                                      system: 'client',
                                      correlation: {
                                        parentField: ['group_id'],
                                        childField: ['id'],
                                      },
                                      subquery: {
                                        table: 'school_group',
                                        alias: 'zsubq_group',
                                        where: {
                                          type: 'correlatedSubquery',
                                          related: {
                                            system: 'client',
                                            correlation: {
                                              parentField: ['id'],
                                              childField: ['group_id'],
                                            },
                                            subquery: {
                                              table: 'school',
                                              alias: 'zsubq_schools',
                                              where: {
                                                type: 'correlatedSubquery',
                                                related: {
                                                  system: 'client',
                                                  correlation: {
                                                    parentField: ['id'],
                                                    childField: ['school_id'],
                                                  },
                                                  subquery: {
                                                    table: 'teacher',
                                                    alias: 'zsubq_teachers',
                                                    where: {
                                                      type: 'and',
                                                      conditions: [
                                                        {
                                                          type: 'simple',
                                                          left: {
                                                            type: 'column',
                                                            name: 'user_id',
                                                          },
                                                          right: {
                                                            type: 'literal',
                                                            value:
                                                              'user_emu_lag_teacher',
                                                          },
                                                          op: '=',
                                                        },
                                                        {
                                                          type: 'simple',
                                                          left: {
                                                            type: 'column',
                                                            name: 'role',
                                                          },
                                                          right: {
                                                            type: 'literal',
                                                            value:
                                                              'administrator',
                                                          },
                                                          op: '=',
                                                        },
                                                      ],
                                                    },
                                                  },
                                                },
                                                op: 'EXISTS',
                                                flip: true,
                                              },
                                            },
                                          },
                                          op: 'EXISTS',
                                          flip: true,
                                        },
                                      },
                                    },
                                    op: 'EXISTS',
                                    flip: true,
                                  },
                                },
                              },
                              op: 'EXISTS',
                              flip: true,
                            },
                          },
                        },
                        op: 'EXISTS',
                        flip: true,
                      },
                    },
                  },
                  op: 'EXISTS',
                  flip: true,
                },
                {
                  type: 'and',
                  conditions: [
                    {
                      type: 'simple',
                      left: {
                        type: 'column',
                        name: 'creation_reason',
                      },
                      right: {
                        type: 'literal',
                        value: 'instant-practice',
                      },
                      op: '=',
                    },
                    {
                      type: 'correlatedSubquery',
                      related: {
                        system: 'client',
                        correlation: {
                          parentField: ['id'],
                          childField: ['assignment_id'],
                        },
                        subquery: {
                          table: 'assignment_to_student',
                          alias: 'zsubq_students',
                          where: {
                            type: 'correlatedSubquery',
                            related: {
                              system: 'client',
                              correlation: {
                                parentField: ['student_id'],
                                childField: ['id'],
                              },
                              subquery: {
                                table: 'student',
                                alias: 'zsubq_student',
                                where: {
                                  type: 'correlatedSubquery',
                                  related: {
                                    system: 'client',
                                    correlation: {
                                      parentField: ['id'],
                                      childField: ['student_id'],
                                    },
                                    subquery: {
                                      table: 'student_class_membership',
                                      alias: 'zsubq_classes',
                                      where: {
                                        type: 'correlatedSubquery',
                                        related: {
                                          system: 'client',
                                          correlation: {
                                            parentField: ['class_id'],
                                            childField: ['id'],
                                          },
                                          subquery: {
                                            table: 'class',
                                            alias: 'zsubq_class',
                                            where: {
                                              type: 'correlatedSubquery',
                                              related: {
                                                system: 'client',
                                                correlation: {
                                                  parentField: ['id'],
                                                  childField: ['class_id'],
                                                },
                                                subquery: {
                                                  table: 'teacher_class_access',
                                                  alias: 'zsubq_teacher_access',
                                                  where: {
                                                    type: 'correlatedSubquery',
                                                    related: {
                                                      system: 'client',
                                                      correlation: {
                                                        parentField: [
                                                          'teacher_id',
                                                        ],
                                                        childField: ['id'],
                                                      },
                                                      subquery: {
                                                        table: 'teacher',
                                                        alias: 'zsubq_teacher',
                                                        where: {
                                                          type: 'simple',
                                                          left: {
                                                            type: 'column',
                                                            name: 'user_id',
                                                          },
                                                          right: {
                                                            type: 'literal',
                                                            value:
                                                              'user_emu_lag_teacher',
                                                          },
                                                          op: '=',
                                                        },
                                                      },
                                                    },
                                                    op: 'EXISTS',
                                                    flip: true,
                                                  },
                                                },
                                              },
                                              op: 'EXISTS',
                                              flip: true,
                                            },
                                          },
                                        },
                                        op: 'EXISTS',
                                      },
                                    },
                                  },
                                  op: 'EXISTS',
                                },
                              },
                            },
                            op: 'EXISTS',
                          },
                        },
                      },
                      op: 'EXISTS',
                    },
                  ],
                },
              ],
            },
          },
        },
        op: 'EXISTS',
        scalar: true,
      },
    ],
  },
  related: [
    {
      correlation: {
        parentField: ['id'],
        childField: ['problem_tracker_id'],
      },
      subquery: {
        table: 'conversation',
        alias: 'conversations',
        limit: 1,
        orderBy: [['created_at', 'desc']],
      },
      system: 'client',
    },
    {
      correlation: {
        parentField: ['id'],
        childField: ['problem_tracker_id'],
      },
      subquery: {
        table: 'mastery_assessment',
        alias: 'mastery_assessment',
      },
      system: 'client',
    },
  ],
};
