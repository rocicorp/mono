import {expect, test} from 'vitest';
import type {Emoji} from './components/emoji-panel.js';
import {formatEmojiCreatorList, formatEmojiTooltipText} from './emoji-utils.js';

function makeEmoji(userID: string, login: string): Emoji {
  return {
    id: 'id',
    created: Date.now(),
    creatorID: userID,
    annotation: 'waves hand',
    subjectID: 'subject-id',
    value: '👋',
    creator: {
      id: userID,
      login,
      avatar: 'avatar',
      name: 'James Holden',
      role: 'admin',
    },
  };
}

test('formatEmojiCreatorList', () => {
  expect(() => formatEmojiCreatorList([], 'me-id')).toThrow();

  // Missing creator
  const badEmoji = makeEmoji('a', 'holden');
  badEmoji.creator = undefined;
  expect(() => formatEmojiCreatorList([badEmoji], 'me-id')).toThrow();

  expect(formatEmojiCreatorList([makeEmoji('a', 'holden')], 'b')).toBe(
    'holden',
  );

  expect(formatEmojiCreatorList([makeEmoji('a', 'holden')], 'a')).toBe('you');

  expect(
    formatEmojiCreatorList(
      [makeEmoji('a', 'holden'), makeEmoji('b', 'naomi')],
      'a',
    ),
  ).toBe('you and naomi');
  expect(
    formatEmojiCreatorList(
      [makeEmoji('a', 'holden'), makeEmoji('b', 'naomi')],
      'b',
    ),
  ).toBe('holden and you');
  expect(
    formatEmojiCreatorList(
      [
        makeEmoji('a', 'holden'),
        makeEmoji('b', 'naomi'),
        makeEmoji('c', 'amos'),
      ],
      'a',
    ),
  ).toBe('you, naomi and amos');
  expect(
    formatEmojiCreatorList(
      [
        makeEmoji('a', 'holden'),
        makeEmoji('b', 'naomi'),
        makeEmoji('c', 'amos'),
        makeEmoji('d', 'alex'),
      ],
      'a',
    ),
  ).toBe('you, naomi, amos and alex');

  // Try with 9 emojis
  expect(
    formatEmojiCreatorList(
      [
        makeEmoji('a', 'holden'),
        makeEmoji('b', 'naomi'),
        makeEmoji('c', 'amos'),
        makeEmoji('d', 'alex'),
        makeEmoji('e', 'bobbi'),
        makeEmoji('f', 'chrisjen'),
        makeEmoji('g', 'dawes'),
        makeEmoji('h', 'elvi'),
        makeEmoji('i', 'fred'),
      ],
      'a',
    ),
  ).toBe('you, naomi, amos, alex, bobbi, chrisjen, dawes, elvi and fred');

  // 10
  expect(
    formatEmojiCreatorList(
      [
        makeEmoji('a', 'holden'),
        makeEmoji('b', 'naomi'),
        makeEmoji('c', 'amos'),
        makeEmoji('d', 'alex'),
        makeEmoji('e', 'bobbi'),
        makeEmoji('f', 'chrisjen'),
        makeEmoji('g', 'dawes'),
        makeEmoji('h', 'elvi'),
        makeEmoji('i', 'fred'),
        makeEmoji('j', 'guy'),
      ],
      'a',
    ),
  ).toBe('you, naomi, amos, alex, bobbi, chrisjen, dawes, elvi, fred and guy');

  // 11
  expect(
    formatEmojiCreatorList(
      [
        makeEmoji('a', 'holden'),
        makeEmoji('b', 'naomi'),
        makeEmoji('c', 'amos'),
        makeEmoji('d', 'alex'),
        makeEmoji('e', 'bobbi'),
        makeEmoji('f', 'chrisjen'),
        makeEmoji('g', 'dawes'),
        makeEmoji('h', 'elvi'),
        makeEmoji('i', 'fred'),
        makeEmoji('j', 'guy'),
        makeEmoji('k', 'havelock'),
      ],
      'a',
    ),
  ).toBe(
    'you, naomi, amos, alex, bobbi, chrisjen, dawes, elvi, fred, guy and havelock',
  );

  // 55
  expect(
    formatEmojiCreatorList(
      Array.from({length: 55}, (_, i) => makeEmoji('id-' + i, 'user-' + i)),
      'anon',
    ),
  ).toBe(
    'user-0, user-1, user-2, user-3, user-4, user-5, user-6, user-7, user-8, user-9 and 45 others',
  );
  expect(
    formatEmojiCreatorList(
      Array.from({length: 55}, (_, i) => makeEmoji('id-' + i, 'user-' + i)),
      'id-5',
    ),
  ).toBe(
    'user-0, user-1, user-2, user-3, user-4, you, user-6, user-7, user-8, user-9 and 45 others',
  );
});

test('formatEmojiTooltipText', () => {
  expect(() => formatEmojiTooltipText([], 'me-id')).toThrow();

  expect(formatEmojiTooltipText([makeEmoji('a', 'holden')], 'b')).toBe(
    'holden reacted with waves hand',
  );
});
