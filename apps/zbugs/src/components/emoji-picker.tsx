import {useZero} from '@rocicorp/zero/react';
import 'emoji-picker-element';
import Database from 'emoji-picker-element/database.js';
import type Picker from 'emoji-picker-element/picker.js';
import type {
  EmojiClickEvent,
  NativeEmoji,
  SkinToneChangeEvent,
} from 'emoji-picker-element/shared.js';
import {createElement, memo, useRef, type RefCallback} from 'react';
import {setUserPref, useUserPref} from '../hooks/use-user-pref.ts';
import {emojiDataSource} from './emoji-data-source.ts';

export const SKIN_TONE_PREF = 'emojiSkinTone';

interface Props {
  onEmojiChange: (emoji: {unicode: string; annotation: string}) => void;
}

export const EmojiPicker = memo(({onEmojiChange}: Props) => {
  const z = useZero();

  // We need to keep a reference to the last picker so we can remove event
  // listeners when the component is unmounted.
  const lastPicker = useRef<Picker | null>(null);

  const skinTonePref = useUserPref(SKIN_TONE_PREF);
  if (skinTonePref !== undefined) {
    const v = parseInt(skinTonePref, 10);
    if (!isNaN(v)) {
      const db = new Database({dataSource: emojiDataSource});
      db.setPreferredSkinTone(v).catch(err => {
        // oxlint-disable-next-line no-console -- Error logging in demo app
        console.error('Failed to set preferred skin tone:', err);
      });
    }
  }

  const onEmojiClick = ({detail}: EmojiClickEvent) => {
    const {unicode} = detail;
    // Custom emojis don't have a unicode property.
    // At this point we don't care about custom emojis.
    if (!unicode) {
      return;
    }
    onEmojiChange({
      unicode: unicode,
      annotation: (detail.emoji as NativeEmoji).annotation,
    });
  };
  const onSkinToneChange = (e: SkinToneChangeEvent) => {
    const skinTone = e.detail.skinTone;
    void setUserPref(z, SKIN_TONE_PREF, skinTone + '');
  };

  // Stop propagation of keypress events to prevent the k/j useKeypress hook to
  // get triggered
  const onKeyPress = (e: Event) => e.stopPropagation();

  const ref: RefCallback<Picker> = el => {
    if (lastPicker.current) {
      lastPicker.current.removeEventListener('emoji-click', onEmojiClick);
      lastPicker.current.removeEventListener(
        'skin-tone-change',
        onSkinToneChange,
      );
      lastPicker.current.removeEventListener('keypress', onKeyPress);
    }
    if (el) {
      el.addEventListener('emoji-click', onEmojiClick);
      el.addEventListener('skin-tone-change', onSkinToneChange);
      el.addEventListener('keypress', onKeyPress);
      lastPicker.current = el;

      // The emoji-picker-element does not allow auto focusing the search input
      // when it's first rendered. We can work around this by observing the
      // shadow DOM and focusing the search input when it's added to the DOM.
      if (el.shadowRoot) {
        const m = new MutationObserver(records => {
          for (const record of records) {
            for (const node of record.addedNodes) {
              if (node.nodeType === Node.ELEMENT_NODE) {
                const search = (node as Element).querySelector('#search');
                if (search) {
                  (search as HTMLElement).focus();
                  m.disconnect();
                }
                return;
              }
            }
          }
        });

        m.observe(el.shadowRoot, {
          subtree: true,
          childList: true,
        });
      }
    }
  };

  return createElement('emoji-picker', {
    'class': 'dark',
    ref,
    'data-source': emojiDataSource,
  });
});
