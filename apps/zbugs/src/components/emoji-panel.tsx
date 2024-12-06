import {
  autoUpdate,
  flip,
  FloatingDelayGroup,
  FloatingFocusManager,
  FloatingPortal,
  shift,
  useDismiss,
  useFloating,
  useInteractions,
  useRole,
  useTransitionStatus,
} from '@floating-ui/react';
import type {Row} from '@rocicorp/zero';
import classNames from 'classnames';
import {nanoid} from 'nanoid';
import {
  forwardRef,
  memo,
  useCallback,
  useEffect,
  useRef,
  useState,
  type ForwardedRef,
} from 'react';
import {toast} from 'react-toastify';
import {useQuery} from 'zero-react/src/use-query.js';
import type {Schema} from '../../schema.js';
import addEmojiIcon from '../assets/icons/add-emoji.svg';
import {formatEmojiCreatorList} from '../emoji-utils.js';
import {useLogin} from '../hooks/use-login.js';
import {useNumericPref} from '../hooks/use-user-pref.js';
import {useZero} from '../hooks/use-zero.js';
import {ButtonWithLoginCheck} from './button-with-login-check.js';
import {type ButtonProps} from './button.js';
import {EmojiPicker, SKIN_TONE_PREF} from './emoji-picker.js';
import {Tooltip, TooltipContent, TooltipTrigger} from './tooltip.jsx';

const loginMessage = 'You need to be logged in to modify emoji reactions.';

export type Emoji = Row<Schema['tables']['emoji']> & {
  creator: Row<Schema['tables']['user']> | undefined;
};

type Props = {
  issueID: string;
  commentID?: string | undefined;
};

export const EmojiPanel = memo(({issueID, commentID}: Props) => {
  const subjectID = commentID ?? issueID;
  const z = useZero();
  const q = z.query.emoji
    .where('subjectID', subjectID)
    .related('creator', creator => creator.one());

  const emojis: Emoji[] = useQuery(q);

  const addEmoji = useCallback(
    (unicode: string, annotation: string) => {
      const id = nanoid();
      z.mutate.emoji.insert({
        id,
        value: unicode,
        annotation,
        subjectID,
        creatorID: z.userID,
        created: Date.now(),
      });
    },
    [subjectID, z],
  );

  const removeEmoji = useCallback(
    (id: string) => {
      z.mutate.emoji.delete({id});
    },
    [z],
  );

  // The emojis is an array. We want to group them by value and count them.
  const groups = groupAndSortEmojis(emojis);

  const addOrRemoveEmoji = useCallback(
    (details: {unicode: string; annotation: string}) => {
      const {unicode, annotation} = details;
      const normalizedEmoji = normalizeEmoji(unicode);
      const emojis = groups[normalizedEmoji] ?? [];
      const existingEmojiID = findEmojiForCreator(emojis, z.userID);
      if (existingEmojiID) {
        removeEmoji(existingEmojiID);
      } else {
        addEmoji(unicode, annotation);
      }
    },
    [addEmoji, groups, removeEmoji, z.userID],
  );

  const login = useLogin();

  const handleEmojiChange = useCallback(
    (changedEmojis: Emoji[]) => {
      console.log('changedEmojis', changedEmojis);
      for (const emoji of changedEmojis) {
        if (emoji.creatorID !== z.userID) {
          toast(
            emoji.creator?.login + ' reacted on a comment: ' + emoji.value,
            {
              position: 'bottom-center',
              containerId: 'bottom',
              className: 'emoji-toast',
              // TODO: Scroll into view and show tooltip where it was added
              closeOnClick: true,
              icon: () => <img className="icon" src={emoji.creator?.avatar} />,
            },
          );
        }
      }
    },
    [z.userID],
  );

  useEmojiChangeListener(emojis, z.userID, handleEmojiChange);

  return (
    <FloatingDelayGroup delay={1000}>
      <div className="flex gap-2 items-center emoji-reaction-container">
        {Object.entries(groups).map(([normalizedEmoji, emojis]) => (
          <EmojiPill
            key={normalizedEmoji}
            normalizedEmoji={normalizedEmoji}
            emojis={emojis}
            addOrRemoveEmoji={addOrRemoveEmoji}
          />
        ))}
        {login.loginState === undefined ? (
          <EmojiButton />
        ) : (
          <EmojiMenuButton onEmojiChange={addOrRemoveEmoji} />
        )}
      </div>
    </FloatingDelayGroup>
  );
});

const EmojiButton = memo(
  forwardRef((props: ButtonProps, ref: ForwardedRef<HTMLButtonElement>) => (
    <ButtonWithLoginCheck
      ref={ref}
      {...props}
      className="add-emoji-button"
      eventName="Add new emoji reaction"
      loginMessage={loginMessage}
    >
      <img src={addEmojiIcon} />
    </ButtonWithLoginCheck>
  )),
);

const EmojiMenuButton = memo(
  ({onEmojiChange}: {onEmojiChange: AddOrRemoveEmoji}) => {
    const [isOpen, setIsOpen] = useState(false);
    const {refs, floatingStyles, placement, context} = useFloating({
      open: isOpen,
      onOpenChange: setIsOpen,
      middleware: [flip(), shift()],
      placement: 'bottom-start',
      whileElementsMounted: autoUpdate,

      // We don't want to position using transforms because we use transforms for
      // the show/hide animations.
      transform: false,
    });
    const dismiss = useDismiss(context);
    const role = useRole(context);
    const {getReferenceProps, getFloatingProps} = useInteractions([
      dismiss,
      role,
    ]);

    const {isMounted, status} = useTransitionStatus(context);

    const onChange = useCallback(
      (details: {unicode: string; annotation: string}) => {
        setIsOpen(false);
        onEmojiChange(details);
      },
      [onEmojiChange],
    );

    // The instructions explicitly says only render the portal when the popup is
    // rendered. However, if doing that the virtual scrolling jumps around when
    // the portal element is removed
    return (
      <>
        <EmojiButton
          ref={refs.setReference}
          onAction={() => setIsOpen(v => !v)}
          {...getReferenceProps()}
        />
        <FloatingPortal id="root-modal">
          {isMounted && (
            <FloatingFocusManager context={context} modal={true}>
              <div
                className="popover-panel"
                ref={refs.setFloating}
                style={floatingStyles}
                {...getFloatingProps()}
                data-placement={placement}
                data-status={status}
              >
                <EmojiPicker onEmojiChange={onChange} />
              </div>
            </FloatingFocusManager>
          )}
        </FloatingPortal>
      </>
    );
  },
);

function normalizeEmoji(emoji: string): string {
  // Skin tone modifiers range from U+1F3FB to U+1F3FF
  return emoji.replace(/[\u{1F3FB}-\u{1F3FF}]/gu, '');
}

function groupAndSortEmojis(emojis: Emoji[]): Record<string, Emoji[]> {
  // Sort the emojis by creation time. Not sure how to sort this with ZQL.
  const sortedEmojis = [...emojis].sort((a, b) => a.created - b.created);
  const rv: Record<string, Emoji[]> = {};
  for (const emoji of sortedEmojis) {
    const normalizedEmoji = normalizeEmoji(emoji.value);
    if (!rv[normalizedEmoji]) {
      rv[normalizedEmoji] = [];
    }
    rv[normalizedEmoji].push(emoji);
  }

  return rv;
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

function setSkinTone(emoji: string, skinTone: number): string {
  const normalizedEmoji = normalizeEmoji(emoji);
  if (skinTone === 0) {
    return normalizedEmoji;
  }

  // Skin tone modifiers range from U+1F3FB to U+1F3FF
  return normalizedEmoji + String.fromCodePoint(0x1f3fa + skinTone);
}

type AddOrRemoveEmoji = (details: {
  unicode: string;
  annotation: string;
}) => void;

function EmojiPill({
  normalizedEmoji,
  emojis,
  addOrRemoveEmoji,
}: {
  normalizedEmoji: string;
  emojis: Emoji[];
  addOrRemoveEmoji: AddOrRemoveEmoji;
}) {
  const z = useZero();
  const skinTone = useNumericPref(SKIN_TONE_PREF, 0);
  const mine = findEmojiForCreator(emojis, z.userID) !== undefined;

  return (
    <Tooltip>
      <TooltipTrigger>
        <ButtonWithLoginCheck
          className={classNames('emoji-pill', {mine})}
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

      <TooltipContent>
        {formatEmojiCreatorList(emojis, z.userID)}
      </TooltipContent>
    </Tooltip>
  );
}

/**
 *@param delay The amount of time in milliseconds to
 * wait before considering changes to the emojis as being new. This allows
 * ignoring changes due to unstable rendering.
 */
function useEmojiChangeListener(
  emojis: Emoji[],
  userID: string,
  cb: (changedEmojis: Emoji[]) => void,
  delay = 500,
) {
  const initialTime = useRef(Date.now());
  const lastEmojis = useRef(
    new Map<string, Emoji>(emojis.map(emoji => [emoji.id, emoji])),
  );
  useEffect(() => {
    const newEmojis = new Map<string, Emoji>(
      emojis.map(emoji => [emoji.id, emoji]),
    );
    const changedEmojis: Emoji[] = [];
    for (const [id, emoji] of newEmojis) {
      if (!lastEmojis.current.has(id)) {
        changedEmojis.push(emoji);
      }
    }

    lastEmojis.current = newEmojis;

    // Ignore if just rendered/mounted
    if (Date.now() - initialTime.current < delay) {
      return;
    }
    cb(changedEmojis);
  }, [cb, delay, emojis, userID]);
}
