import {
  autoUpdate,
  flip,
  FloatingFocusManager,
  FloatingPortal,
  shift,
  useDismiss,
  useFloating,
  useInteractions,
  useRole,
  useTransitionStatus,
} from '@floating-ui/react';
import * as Tooltip from '@radix-ui/react-tooltip';
import type {TableSchemaToRow} from '@rocicorp/zero';
import classNames from 'classnames';
import {nanoid} from 'nanoid';
import {forwardRef, useCallback, useState, type ForwardedRef} from 'react';
import {useQuery} from 'zero-react/src/use-query.js';
import type {Schema} from '../../schema.js';
import addEmojiIcon from '../assets/icons/add-emoji.svg';
import {formatEmojiTooltipText} from '../emoji-utils.js';
import {useLogin} from '../hooks/use-login.js';
import {useNumericPref} from '../hooks/use-user-pref.js';
import {useZero} from '../hooks/use-zero.js';
import {ButtonWithLoginCheck} from './button-with-login-check.js';
import type {ButtonProps} from './button.js';
import {EmojiPicker, SKIN_TONE_PREF} from './emoji-picker.js';
import './emoji.css';

const loginMessage = 'You need to be logged in to modify emoji reactions.';

export type Emoji = TableSchemaToRow<Schema['tables']['emoji']> & {
  creator: TableSchemaToRow<Schema['tables']['user']> | undefined;
};

type Props = {
  issueID: string;
  commentID?: string | undefined;
};

export function EmojiPanel({issueID, commentID}: Props) {
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

  return (
    <div className="flex gap-2 items-center emoji-reaction-container">
      <Tooltip.Provider>
        {Object.entries(groups).map(([normalizedEmoji, emojis]) => (
          <EmojiPill
            key={normalizedEmoji}
            normalizedEmoji={normalizedEmoji}
            emojis={emojis}
            addOrRemoveEmoji={addOrRemoveEmoji}
          />
        ))}
      </Tooltip.Provider>
      {login.loginState === undefined ? (
        <EmojiButton />
      ) : (
        <EmojiMenuButton onEmojiChange={addOrRemoveEmoji} />
      )}
    </div>
  );
}

const EmojiButton = forwardRef(
  (props: ButtonProps, ref: ForwardedRef<HTMLButtonElement>) => (
    <ButtonWithLoginCheck
      ref={ref}
      {...props}
      className="add-emoji-button"
      eventName="Add new emoji reaction"
      loginMessage={loginMessage}
    >
      <img src={addEmojiIcon} />
    </ButtonWithLoginCheck>
  ),
);

function EmojiMenuButton({onEmojiChange}: {onEmojiChange: AddOrRemoveEmoji}) {
  const [isOpen, setIsOpen] = useState(false);
  const {refs, floatingStyles, placement, context} = useFloating({
    open: isOpen,
    onOpenChange: setIsOpen,
    middleware: [flip(), shift()],
    placement: 'bottom-start',
    whileElementsMounted: autoUpdate,
    // strategy: 'fixed',
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
}

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
    <Tooltip.Root>
      <Tooltip.Trigger asChild>
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
      </Tooltip.Trigger>
      <Tooltip.Portal>
        <Tooltip.Content className="tooltip-content">
          <Tooltip.Arrow className="tooltip-arrow" />
          <EmojiTooltipContent emojis={emojis} userID={z.userID} />
        </Tooltip.Content>
      </Tooltip.Portal>
    </Tooltip.Root>
  );
}

function EmojiTooltipContent({
  emojis,
  userID,
}: {
  emojis: Emoji[];
  userID: string;
}) {
  const emoji = emojis[0];
  return (
    <>
      <div className="emoji-icon">{emoji.value}</div>
      <div>{formatEmojiTooltipText(emojis, userID)}</div>
    </>
  );
}
