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
import {useZero} from '@rocicorp/zero/react';
import {nanoid} from 'nanoid';
import {
  forwardRef,
  memo,
  useCallback,
  useMemo,
  useState,
  type ForwardedRef,
} from 'react';
import {mutators} from '../../shared/mutators.ts';
import addEmojiIcon from '../assets/icons/add-emoji.svg';
import {
  findEmojiForCreator,
  normalizeEmoji,
  type Emoji,
} from '../emoji-utils.ts';
import {useLogin} from '../hooks/use-login.tsx';
import {ButtonWithLoginCheck} from './button-with-login-check.tsx';
import {type ButtonProps} from './button.tsx';
import {EmojiPicker} from './emoji-picker.tsx';
import {EmojiPill} from './emoji-pill.tsx';

const loginMessage = 'You need to be logged in to modify emoji reactions.';

type Props = {
  issueID: string;
  commentID?: string | undefined;
  emojis: readonly Emoji[];
  recentEmojis?: readonly Emoji[] | undefined;
  removeRecentEmoji?: ((id: string) => void) | undefined;
};

export const EmojiPanel = memo(
  forwardRef(
    (
      {issueID, commentID, emojis, recentEmojis, removeRecentEmoji}: Props,
      ref: ForwardedRef<HTMLDivElement>,
    ) => {
      const subjectID = commentID ?? issueID;
      const z = useZero();

      const addEmoji = useCallback(
        (unicode: string, annotation: string) => {
          const args = {
            id: nanoid(),
            value: unicode,
            annotation,
            subjectID,
            creatorID: z.userID,
            created: Date.now(),
          } as const;
          if (commentID !== undefined) {
            z.mutate(mutators.emoji.addToComment(args));
          } else {
            z.mutate(mutators.emoji.addToIssue(args));
          }
        },
        [subjectID, commentID, z],
      );

      const removeEmoji = useCallback(
        (id: string) => {
          z.mutate(mutators.emoji.remove({id}));
        },
        [z],
      );

      // The emojis is an array. We want to group them by value and count them.
      const groups = useMemo(() => groupAndSortEmojis(emojis), [emojis]);

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
        <FloatingDelayGroup delay={1000}>
          <div className="emoji-reaction-container" ref={ref}>
            {Object.entries(groups).map(([normalizedEmoji, emojis]) => (
              <EmojiPill
                key={normalizedEmoji}
                normalizedEmoji={normalizedEmoji}
                emojis={emojis}
                addOrRemoveEmoji={addOrRemoveEmoji}
                recentEmojis={recentEmojis}
                removeRecentEmoji={removeRecentEmoji}
                subjectID={subjectID}
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
    },
  ),
);

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

function groupAndSortEmojis(emojis: readonly Emoji[]): Record<string, Emoji[]> {
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

type AddOrRemoveEmoji = (details: {
  unicode: string;
  annotation: string;
}) => void;
