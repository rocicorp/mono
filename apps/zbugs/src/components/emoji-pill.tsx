import {useQuery} from '@rocicorp/zero/react';
import classNames from 'classnames';
import {useEffect, useState} from 'react';
import {useIntersectionObserver} from 'usehooks-ts';
import type {Emoji} from '../emoji-utils.js';
import {
  findEmojiForCreator,
  formatEmojiCreatorList,
  setSkinTone,
} from '../emoji-utils.js';
import {useDocumentHasFocus} from '../hooks/use-document-has-focus.js';
import {useNumericPref} from '../hooks/use-user-pref.js';
import {useZero} from '../hooks/use-zero.js';
import {ButtonWithLoginCheck} from './button-with-login-check.js';
import {SKIN_TONE_PREF} from './emoji-picker.js';
import {Tooltip, TooltipContent, TooltipTrigger} from './tooltip.jsx';

const loginMessage = 'You need to be logged in to modify emoji reactions.';

const triggeredTooltipDuration = 1_000;

type AddOrRemoveEmoji = (details: {
  unicode: string;
  annotation: string;
}) => void;

type Props = {
  normalizedEmoji: string;
  emojis: Emoji[];
  addOrRemoveEmoji: AddOrRemoveEmoji;
  recentEmojiIDs?: readonly string[] | undefined;
  removeRecentEmoji?: ((id: string) => void) | undefined;
  subjectID: string;
};

export function EmojiPill({
  normalizedEmoji,
  emojis,
  addOrRemoveEmoji,
  recentEmojiIDs,
  removeRecentEmoji,
  subjectID,
}: Props) {
  const z = useZero();
  const skinTone = useNumericPref(SKIN_TONE_PREF, 0);
  const mine = findEmojiForCreator(emojis, z.userID) !== undefined;
  const [forceShow, setForceShow] = useState(false);
  const [wasTriggered, setWasTriggered] = useState(false);
  const [triggeredEmojiIDs, setTriggeredEmojiIDs] = useState<string[]>([]);
  const {isIntersecting, ref} = useIntersectionObserver({
    threshold: 0.5,
    freezeOnceVisible: true,
  });
  const documentHasFocus = useDocumentHasFocus();

  useEffect(() => {
    if (!recentEmojiIDs) {
      return;
    }
    const newTriggeredEmojiIDs: string[] = [];
    for (const id of recentEmojiIDs) {
      if (emojis.some(e => e.id === id)) {
        setWasTriggered(true);
        newTriggeredEmojiIDs.push(id);
      }
    }
    setTriggeredEmojiIDs(newTriggeredEmojiIDs);
  }, [emojis, recentEmojiIDs, subjectID]);

  useEffect(() => {
    if (wasTriggered && isIntersecting && !forceShow) {
      setForceShow(true);
    }
  }, [isIntersecting, forceShow, wasTriggered]);

  useEffect(() => {
    if (forceShow && documentHasFocus && removeRecentEmoji) {
      const timer = setTimeout(() => {
        setForceShow(false);
        setWasTriggered(false);
        const [firstID, ...restIDs] = triggeredEmojiIDs;
        if (firstID) {
          removeRecentEmoji(firstID);
        }
        setTriggeredEmojiIDs(restIDs);
      }, triggeredTooltipDuration);

      return () => clearTimeout(timer);
    }
    return () => void 0;
  }, [triggeredEmojiIDs, documentHasFocus, forceShow, removeRecentEmoji]);

  const triggered = triggeredEmojiIDs.length > 0;

  return (
    <Tooltip open={forceShow || undefined}>
      <TooltipTrigger>
        <ButtonWithLoginCheck
          ref={ref}
          className={classNames('emoji-pill', {
            mine,
            triggered,
          })}
          eventName="Add to existing emoji reaction"
          key={normalizedEmoji}
          loginMessage={loginMessage}
          onAction={() =>
            addOrRemoveEmoji({
              unicode: setSkinTone(normalizedEmoji, skinTone),
              annotation: emojis[0].annotation ?? '',
            })
          }
        >
          {unique(emojis).map(value => (
            <span key={value}>{value}</span>
          ))}
          {' ' + emojis.length}
        </ButtonWithLoginCheck>
      </TooltipTrigger>

      <TooltipContent className={classNames({triggered})}>
        {triggeredEmojiIDs.length > 0 ? (
          <TriggeredTooltipContent emojiIDs={triggeredEmojiIDs} />
        ) : (
          formatEmojiCreatorList(emojis, z.userID)
        )}
      </TooltipContent>
    </Tooltip>
  );
}

function TriggeredTooltipContent({emojiIDs}: {emojiIDs: string[]}) {
  const emojiID = emojiIDs[0];
  const z = useZero();
  const [emoji] = useQuery(
    z.query.emoji
      .where('id', emojiID)
      .related('creator', creator => creator.one())
      .one(),
  );
  if (!emoji || !emoji.creator) {
    return null;
  }

  return (
    <>
      <img className="tooltip-emoji-icon" src={emoji.creator.avatar} />
      {emoji.creator.login}
    </>
  );
}

function unique(emojis: Emoji[]): string[] {
  return [...new Set(emojis.map(emoji => emoji.value))];
}
