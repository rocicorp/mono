import {assert} from 'shared/src/asserts.js';
import type {Emoji} from './components/emoji-panel.js';

export function formatEmojiCreatorList(
  emojis: Emoji[],
  currentUserID: string,
): string {
  assert(emojis.length > 0);
  const names = emojis.map(emoji => {
    const {creator} = emoji;
    assert(creator);
    if (emoji.creatorID === currentUserID) {
      return 'you';
    }
    return creator.login;
  });
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
