import 'emoji-picker-element';
import {createElement, type RefCallback} from 'react';

interface Props {
  onChange: (emoji: string) => void;
}
export function EmojiPicker({onChange}: Props) {
  const ref: RefCallback<HTMLElement> = el => {
    if (el) {
      const f = (e: Event) => {
        onChange((e as CustomEvent).detail.unicode);
      };
      el.addEventListener('emoji-click', f);
    }
  };

  return createElement('emoji-picker', {class: 'dark', ref});
}
