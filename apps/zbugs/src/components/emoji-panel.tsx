import {
  Popover,
  PopoverButton,
  PopoverPanel,
  useClose,
} from '@headlessui/react';
import {nanoid} from 'nanoid';
import {useCallback} from 'react';
import addEmojiIcon from '../assets/icons/add-emoji.svg';
import {useZero} from '../hooks/use-zero.js';
import {Button} from './button.js';
import {EmojiPicker} from './emoji-picker.js';

type Emoji = {
  id: string;
  value: string;
  creatorID: string;
  created: number;
  creator: {
    login: string;
  };
};

type Props = {
  emojis: Emoji[];
  issueID: string;
  commentID?: string | undefined;
};

export function EmojiPanel({emojis, issueID, commentID}: Props) {
  const z = useZero();

  const addEmoji = useCallback(
    (emoji: string) => {
      const id = nanoid();
      z.mutate(m => {
        m.emoji.create({
          id,
          value: emoji,
          creatorID: z.userID,
          created: Date.now(),
        });

        if (commentID) {
          m.commentEmoji.create({
            commentID,
            emojiID: id,
          });
        } else {
          m.issueEmoji.create({
            issueID,
            emojiID: id,
          });
        }

        // also update the modified time of the issue
        m.issue.update({id: issueID, modified: Date.now()});
      });
    },
    [commentID, issueID, z],
  );

  const removeEmoji = useCallback(
    (id: string) => {
      z.mutate(m => {
        m.emoji.delete({id});

        if (commentID) {
          m.commentEmoji.delete({commentID, emojiID: id});
        } else {
          m.issueEmoji.delete({issueID, emojiID: id});
        }

        // also update the modified time of the issue
        m.issue.update({id: issueID, modified: Date.now()});
      });
    },
    [commentID, issueID, z],
  );

  // The emojis is an array. We want to group them by value and count them.
  const groups = groupEmojis(emojis);

  const addOrRemoveEmoji = useCallback(
    (emoji: string) => {
      const normalizedEmoji = normalizeEmoji(emoji);
      const emojis = groups[normalizedEmoji] ?? [];
      const existingEmojiID = findEmojiForCreator(emojis, z.userID);
      if (existingEmojiID) {
        removeEmoji(existingEmojiID);
      } else {
        addEmoji(normalizedEmoji);
      }
    },
    [addEmoji, groups, removeEmoji, z.userID],
  );

  return (
    <div className="flex gap-2 items-center">
      {Object.entries(groups).map(([normalizedEmoji, emojis]) => (
        <Button
          className="emoji-pill"
          key={normalizedEmoji}
          title={'TODO: Who reacted with this emoji'}
          onAction={() => addOrRemoveEmoji(normalizedEmoji)}
        >
          {unique(emojis).map(value => (
            <span key={value}>{value}</span>
          ))}
          {emojis.length > 1 ? ' ' + emojis.length : ''}
        </Button>
      ))}
      <Popover>
        <PopoverButton as="div">
          <Button className="add-emoji-button">
            <img src={addEmojiIcon} />
          </Button>
        </PopoverButton>
        <PopoverPanel anchor="bottom start">
          <PopoverContent
            onChange={emoji => {
              addOrRemoveEmoji(emoji);
            }}
          />
        </PopoverPanel>
      </Popover>
    </div>
  );
}

function PopoverContent({onChange}: {onChange: (emoji: string) => void}) {
  const close = useClose();
  return (
    <EmojiPicker
      onChange={x => {
        onChange(x);
        close();
      }}
    />
  );
}

function normalizeEmoji(emoji: string): string {
  // Skin tone modifiers range from U+1F3FB to U+1F3FF
  return emoji.replace(/[\u{1F3FB}-\u{1F3FF}]/gu, '');
}

function groupEmojis(emojis: Emoji[]): Record<string, Emoji[]> {
  return Object.groupBy(emojis, emoji => normalizeEmoji(emoji.value)) as Record<
    string,
    Emoji[]
  >;
}

function findEmojiForCreator(
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

function unique(emojis: Emoji[]): string[] {
  return [...new Set(emojis.map(emoji => emoji.value))];
}
