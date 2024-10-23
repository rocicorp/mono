import 'emoji-picker-element';
import Database from 'emoji-picker-element/database.js';
import type Picker from 'emoji-picker-element/picker.js';
import {createElement, type RefCallback} from 'react';
import {setUserPref, useUserPref} from '../hooks/use-user-pref.js';
import {useZero} from '../hooks/use-zero.js';

export const SKIN_TONE_PREF = 'emojiSkinTone';

interface Props {
  onEmojiChange: (emoji: {unicode: string; annotation: string}) => void;
}

export function EmojiPicker({onEmojiChange}: Props) {
  const z = useZero();

  const skinTonePref = useUserPref(SKIN_TONE_PREF);
  if (skinTonePref !== undefined) {
    const v = parseInt(skinTonePref, 10);
    if (!isNaN(v)) {
      const db = new Database();
      db.setPreferredSkinTone(v).catch(err => {
        console.error('Failed to set preferred skin tone:', err);
      });
    }
  }

  const saveSkinTone = async (skinTone: number) => {
    await setUserPref(z, SKIN_TONE_PREF, skinTone.toString());
  };

  const ref: RefCallback<Picker> = el => {
    if (el) {
      el.addEventListener('emoji-click', e =>
        onEmojiChange({
          unicode: e.detail.unicode,
          annotation: e.detail.emoji.annotation,
        }),
      );
      el.addEventListener('skin-tone-change', e => {
        saveSkinTone(e.detail.skinTone);
      });
    }
  };

  return createElement('emoji-picker', {class: 'dark', ref});
}
