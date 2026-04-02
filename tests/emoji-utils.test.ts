import { formatEmojiCreatorList, formatEmojiTooltipText, setSkinTone, findEmojiForCreator, normalizeEmoji } from '../apps/zbugs/src/emoji-utils';

describe('emoji-utils', () => {
  describe('formatEmojiCreatorList', () => {
    it('should handle single emoji with creator', () => {
      const emojis = [{
        id: '1',
        annotation: 'thumbsup',
        creatorID: 'user1',
        creator: { login: 'alice' },
      }];
      expect(formatEmojiCreatorList(emojis, 'user2')).toBe('alice');
    });

    it('should handle single emoji with current user', () => {
      const emojis = [{
        id: '1',
        annotation: 'thumbsup',
        creatorID: 'user1',
        creator: { login: 'alice' },
      }];
      expect(formatEmojiCreatorList(emojis, 'user1')).toBe('you');
    });

    it('should handle multiple emojis', () => {
      const emojis = [
        {
          id: '1',
          annotation: 'thumbsup',
          creatorID: 'user1',
          creator: { login: 'alice' },
        },
        {
          id: '2',
          annotation: 'thumbsup',
          creatorID: 'user2',
          creator: { login: 'bob' },
        },
      ];
      expect(formatEmojiCreatorList(emojis, 'user3')).toBe('alice and bob');
    });

    it('should handle more than 11 emojis', () => {
      const emojis = Array.from({ length: 15 }, (_, i) => ({
        id: `${i + 1}`,
        annotation: 'thumbsup',
        creatorID: `user${i + 1}`,
        creator: { login: `user${i + 1}` },
      }));
      expect(formatEmojiCreatorList(emojis, 'user1')).toBe(
        'user1, user2, user3, user4, user5, user6, user7, user8, user9, user10 and 5 others'
      );
    });

    it('should handle emojis without creators', () => {
      const emojis = [
        {
          id: '1',
          annotation: 'thumbsup',
          creatorID: 'user1',
          creator: { login: 'alice' },
        },
        {
          id: '2',
          annotation: 'thumbsup',
          creatorID: 'user2',
          creator: undefined,
        },
      ];
      expect(formatEmojiCreatorList(emojis, 'user3')).toBe('alice');
    });

    it('should throw assertion error for empty emojis', () => {
      expect(() => formatEmojiCreatorList([], 'user1')).toThrow('Expected at least one emoji');
    });
  });

  describe('formatEmojiTooltipText', () => {
    it('should handle empty emojis', () => {
      expect(formatEmojiTooltipText([], 'user1')).toBe('');
    });

    it('should handle single emoji', () => {
      const emojis = [{
        id: '1',
        annotation: 'thumbsup',
        creatorID: 'user1',
        creator: { login: 'alice' },
      }];
      expect(formatEmojiTooltipText(emojis, 'user2')).toBe('alice reacted with thumbsup');
    });

    it('should handle multiple emojis', () => {
      const emojis = [
        {
          id: '1',
          annotation: 'thumbsup',
          creatorID: 'user1',
          creator: { login: 'alice' },
        },
        {
          id: '2',
          annotation: 'thumbsup',
          creatorID: 'user2',
          creator: { login: 'bob' },
        },
      ];
      expect(formatEmojiTooltipText(emojis, 'user3')).toBe('alice and bob reacted with thumbsup');
    });
  });

  describe('setSkinTone', () => {
    it('should return original emoji for skin tone 0', () => {
      expect(setSkinTone('👍', 0)).toBe('👍');
    });

    it('should add skin tone modifier', () => {
      expect(setSkinTone('👍', 1)).toBe('👍🏻');
    });
  });

  describe('findEmojiForCreator', () => {
    it('should find emoji by creator ID', () => {
      const emojis = [
        {
          id: '1',
          annotation: 'thumbsup',
          creatorID: 'user1',
          creator: { login: 'alice' },
        },
        {
          id: '2',
          annotation: 'thumbsup',
          creatorID: 'user2',
          creator: { login: 'bob' },
        },
      ];
      expect(findEmojiForCreator(emojis, 'user1')).toBe('1');
    });

    it('should return undefined if no matching creator', () => {
      const emojis = [
        {
          id: '1',
          annotation: 'thumbsup',
          creatorID: 'user1',
          creator: { login: 'alice' },
        },
      ];
      expect(findEmojiForCreator(emojis, 'user2')).toBeUndefined();
    });
  });

  describe('normalizeEmoji', () => {
    it('should remove skin tone modifiers', () => {
      expect(normalizeEmoji('👍🏻')).toBe('👍');
    });

    it('should handle emoji without skin tone', () => {
      expect(normalizeEmoji('👍')).toBe('👍');
    });
  });
});