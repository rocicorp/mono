import {expect, suite, test} from 'vitest';
import type {EntityID} from 'zero-protocol/src/entity.js';
import {toEntitiesKey} from './keys.js';

suite('toEntitiesKey', () => {
  const cases: {entityType: string; entityID: EntityID; expectedKey: string}[] =
    [
      {
        entityType: 'issue',
        entityID: {id: 'issue1'},
        expectedKey: 'e/issue/issue1',
      },
      {
        entityType: 'issue_label',
        entityID: {issueID: 'issue1', labelID: 'label1'},
        expectedKey: 'e/issue_label/{"issueID":"issue1","labelID":"label1"}',
      },
      // demonstrate sort on attribute name
      {
        entityType: 'issue_label',
        entityID: {labelID: 'label1', issueID: 'issue1'},
        expectedKey: 'e/issue_label/{"issueID":"issue1","labelID":"label1"}',
      },
    ];
  for (const {entityType, entityID, expectedKey} of cases) {
    test(`${entityType} ${JSON.stringify(entityID)} => ${expectedKey}`, () => {
      expect(toEntitiesKey(entityType, entityID)).toEqual(expectedKey);
    });
  }
});
