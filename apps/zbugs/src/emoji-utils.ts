import {assert} from 'shared/src/asserts.js';
import type {ZbugsRow} from '../shared/zero-hooks.ts';

export type Emoji = ZbugsRow['emoji'] & {
  readonly creator: ZbugsRow['user'] | undefined;
};

export function formatEmojiCreatorList(
  emojis: Emoji[],
  currentUserID: string,
): string {
  assert(emojis.length > 0);
  const names = emojis
    .filter(emoji => emoji.creator !== undefined)
    .map(emoji => {
      const {creator} = emoji;
      assert(creator);
      if (emoji.creatorID === currentUserID) {
        return 'you';
      }
      return creator.login;
    });

  if (names.length === 0) {
    return '';
  }
  if (names.length === 1) {
    return names[0];
  }

  if (names.length > 11) {
    return (
      names.slice(0, 10).join(', ') + ' and ' + (names.length - 10) + ' others'
    );
  }

  return names.slice(0, -1).join(', ') + ' and ' + names.slice(-1);
}

export function formatEmojiTooltipText(
  emojis: Emoji[],
  currentUserID: string,
): string {
  const names = formatEmojiCreatorList(emojis, currentUserID);
  return `${names} reacted with ${emojis[0].annotation}`;
}

export function setSkinTone(emoji: string, skinTone: number): string {
  const normalizedEmoji = normalizeEmoji(emoji);
  if (skinTone === 0) {
    return normalizedEmoji;
  }

  // Skin tone modifiers range from U+1F3FB to U+1F3FF
  return normalizedEmoji + String.fromCodePoint(0x1f3fa + skinTone);
}

export function findEmojiForCreator(
  emojis: Emoji[],
  userID: string,
): string | undefined {
  for (const emoji of emojis) {
    if (emoji.creatorID === userID) {
      return emoji.id;
    }
  }
  return undefined;
}

export function normalizeEmoji(emoji: string): string {
  // Skin tone modifiers range from U+1F3FB to U+1F3FF
  return emoji.replace(/[\u{1F3FB}-\u{1F3FF}]/gu, '');
}
