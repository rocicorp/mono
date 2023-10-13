import {describe, test, expect} from '@jest/globals';
import {parseScriptTags, type ScriptTags} from './script-tags.js';

describe('script tags', () => {
  type Case = {
    name: string;
    tags: string[];
    result?: ScriptTags;
  };
  const cases: Case[] = [
    {
      name: 'no tags',
      tags: [],
    },
    {
      name: 'incomplete tags',
      tags: ['appID:foo', 'appName:bar', 'teamID:baz'],
    },
    {
      name: 'all tags',
      tags: ['appID:foo', 'appName:bar', 'teamID:baz', 'teamLabel:bonk'],
      result: {
        appID: 'foo',
        appName: 'bar',
        teamID: 'baz',
        teamLabel: 'bonk',
      },
    },
    {
      name: 'tags with colons',
      tags: [
        'appID:foo:bar',
        'appName:bar:baz',
        'teamID:baz:bonk',
        'teamLabel:bonk:foo',
      ],
      result: {
        appID: 'foo:bar',
        appName: 'bar:baz',
        teamID: 'baz:bonk',
        teamLabel: 'bonk:foo',
      },
    },
  ];
  for (const c of cases) {
    test(c.name, () => {
      let error;
      let result;
      try {
        result = parseScriptTags(c.tags);
      } catch (e) {
        error = e;
      }
      expect(result).toEqual(c.result);
      if (!c.result) {
        expect(error).toBeInstanceOf(TypeError);
      }
    });
  }
});
