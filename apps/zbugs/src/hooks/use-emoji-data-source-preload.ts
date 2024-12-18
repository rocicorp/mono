import Database from 'emoji-picker-element/database.js';
import {useEffect} from 'react';
import {emojiDataSource} from '../components/emoji-data-source.js';

export function useEmojiDataSourcePreload() {
  useEffect(() => {
    // Do this on a timer to not compete with other work.
    const timer = setTimeout(
      () => {
        new Database({dataSource: emojiDataSource});
      },
      1000 + Math.random() * 1000,
    );
    return () => clearTimeout(timer);
  }, []);
}
