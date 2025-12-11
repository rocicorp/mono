import {useZero} from '@rocicorp/zero/react';
import classNames from 'classnames';
import {memo, useEffect, useState} from 'react';
import {useIntersectionObserver} from 'usehooks-ts';
import type {Emoji} from '../emoji-utils.ts';
import {
  findEmojiForCreator,
  formatEmojiCreatorList,
  setSkinTone,
} from '../emoji-utils.ts';
import {useDocumentHasFocus} from '../hooks/use-document-has-focus.ts';
import {useNumericPref} from '../hooks/use-user-pref.ts';
import {AvatarImage} from './avatar-image.tsx';
import {ButtonWithLoginCheck} from './button-with-login-check.tsx';
import {SKIN_TONE_PREF} from './emoji-picker.tsx';
import {Tooltip, TooltipContent, TooltipTrigger} from './tooltip.tsx';

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
  recentEmojis?: readonly Emoji[] | undefined;
  removeRecentEmoji?: ((id: string) => void) | undefined;
  subjectID: string;
};

export const EmojiPill = memo(
  ({
    normalizedEmoji,
    emojis,
    addOrRemoveEmoji,
    recentEmojis,
    removeRecentEmoji,
    subjectID,
  }: Props) => {
    const z = useZero();
    const skinTone = useNumericPref(SKIN_TONE_PREF, 0);
    const mine = findEmojiForCreator(emojis, z.userID) !== undefined;
    const [forceShow, setForceShow] = useState(false);
    const [wasTriggered, setWasTriggered] = useState(false);
    const [triggeredEmojis, setTriggeredEmojis] = useState<Emoji[]>([]);
    const {isIntersecting, ref} = useIntersectionObserver({
      threshold: 0.5,
      freezeOnceVisible: true,
    });
    const documentHasFocus = useDocumentHasFocus();

    useEffect(() => {
      if (!recentEmojis) {
        return;
      }
      const newTriggeredEmojis: Emoji[] = [];
      for (const emoji of recentEmojis) {
        if (emojis.some(e => e.id === emoji.id)) {
          newTriggeredEmojis.push(emoji);
        }
      }
      setWasTriggered(newTriggeredEmojis.length > 0);
      setTriggeredEmojis(newTriggeredEmojis);
    }, [emojis, recentEmojis, subjectID]);

    useEffect(() => {
      if (wasTriggered && isIntersecting && !forceShow) {
        setForceShow(true);
      }
    }, [isIntersecting, forceShow, wasTriggered]);

    useEffect(() => {
      if (forceShow && documentHasFocus && removeRecentEmoji) {
        const id = setTimeout(() => {
          setForceShow(false);
          setWasTriggered(false);
          const [first, ...rest] = triggeredEmojis;
          if (first) {
            removeRecentEmoji(first.id);
          }
          setTriggeredEmojis(rest);
        }, triggeredTooltipDuration);

        return () => clearTimeout(id);
      }
      return () => void 0;
    }, [triggeredEmojis, documentHasFocus, forceShow, removeRecentEmoji]);

    const triggered = triggeredEmojis.length > 0;

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
          {triggeredEmojis.length > 0 ? (
            <TriggeredTooltipContent emojis={triggeredEmojis} />
          ) : (
            formatEmojiCreatorList(emojis, z.userID)
          )}
        </TooltipContent>
      </Tooltip>
    );
  },
);

function TriggeredTooltipContent({emojis}: {emojis: Emoji[]}) {
  const {creator} = emojis[0];
  return (
    creator && (
      <>
        <AvatarImage className="tooltip-emoji-icon" user={creator} />
        {creator.login}
      </>
    )
  );
}

function unique(emojis: Emoji[]): string[] {
  return [...new Set(emojis.map(emoji => emoji.value))];
}
