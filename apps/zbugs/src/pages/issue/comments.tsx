import type {Row} from '@rocicorp/zero';
import {
  rowAttributes,
  useZeroWindowVirtualizer,
  type VirtualRow,
} from '@rocicorp/zero-virtual/react';
import {useQuery, useZero} from '@rocicorp/zero/react';
import {useCallback, useEffect, useRef} from 'react';
import {toast} from 'react-toastify';
import {queries} from '../../../shared/queries.ts';
import {makePermalink} from '../../comment-permalink.ts';
import {AvatarImage} from '../../components/avatar-image.tsx';
import {navigate} from '../../navigate.ts';
import {Comment} from './comment.tsx';
import {ToastContent} from './toast-content.tsx';

export type CommentRow = Row<ReturnType<typeof queries.commentsPage>>;

type CommentStart = {
  readonly id: string;
  readonly created: number;
};

const ESTIMATED_COMMENT_HEIGHT = 200;

// The virtualizer options must keep a stable identity across renders: it
// compares several of them by identity (a changed `estimateSize`/`getRowKey`
// invalidates its snapshot) and calls the query functions on every render to
// stage its Zero queries. Recreating them each render makes paging reset to
// the first page (it never advances) and can spin the render loop. Everything
// that doesn't close over props lives at module scope; the one that needs
// `issueID` is memoized on it below.
const estimateSize = () => ESTIMATED_COMMENT_HEIGHT;
const getRowKey = (row: CommentRow) => row.id;
const toStartRow = (row: CommentRow): CommentStart => ({
  id: row.id,
  created: row.created,
});
const getSingleQuery = ({id}: {id: string}) => ({query: queries.comment(id)});

/**
 * The comments list: zero-virtual's window virtualizer in dynamic
 * (natural-height) mode. Comments page in incrementally as you scroll —
 * nothing is eagerly loaded — and a `#comment-<id>` permalink loads the
 * window around the target and scrolls it into view.
 */
export function Comments({
  issueID,
  permalinkID,
}: {
  issueID: string;
  /** Comment id from a `#comment-<id>` permalink, if any. */
  permalinkID: string | null;
}) {
  const rowsRef = useRef<HTMLDivElement>(null);
  const getScrollElement = useCallback(() => rowsRef.current, []);

  const getPageQuery = useCallback(
    ({
      limit,
      start,
      dir,
    }: {
      limit: number;
      start: CommentStart | null;
      dir: 'forward' | 'backward';
    }) => ({
      query: queries.commentsPage({
        issueID,
        limit,
        start,
        dir,
        inclusive: start === null,
      }),
    }),
    [issueID],
  );

  const {items, spaceBefore, spaceAfter} = useZeroWindowVirtualizer<
    string,
    CommentRow,
    CommentStart
  >({
    listContextParams: issueID,
    getScrollElement,
    estimateSize,
    anchoring: 'native',
    // Comments are tall cards — the default 100-row page floor is many
    // viewports of content and lands as a long task when a page renders.
    // ~3 viewports of ~200px rows is ~14; floor at 20.
    minPageSize: 20,
    getRowKey,
    getPageQuery,
    getSingleQuery,
    toStartRow,
    permalinkID,
  });

  useShowToastForNewComment(issueID);

  return (
    // spaceBefore/spaceAfter render as spacer elements, NOT padding on the
    // container: native scroll anchoring (all engines) does not compensate a
    // padding change on the anchor's ancestor, so paging would jump the
    // viewport. A content box (this div) is compensated.
    <div className="comments-container" ref={rowsRef}>
      <div style={{height: spaceBefore}} />
      {items.map(item => (
        <CommentRowView key={item.key} item={item} issueID={issueID} />
      ))}
      <div style={{height: spaceAfter}} />
    </div>
  );
}

function CommentRowView({
  item,
  issueID,
}: {
  item: VirtualRow<CommentRow>;
  issueID: string;
}) {
  const {index, key, row} = item;
  // The gap goes *inside* the measured element (padding, not margin) so the
  // virtualizer's height measurement includes it and spacing stays
  // consistent.
  const rowStyle = {paddingBottom: 16};
  if (row === undefined) {
    return (
      <div {...rowAttributes(index, key)} style={{...rowStyle, minHeight: 120}}>
        <div className="comment-item skeleton-shimmer">Loading…</div>
      </div>
    );
  }
  return (
    <div {...rowAttributes(index, key)} style={rowStyle}>
      <Comment id={row.id} issueID={issueID} comment={row} />
    </div>
  );
}

/**
 * Toast when someone else posts a comment that lands outside the viewport.
 * Watches the newest comment via a 1-row query (the list itself only loads
 * the window around the scroll position, so it can't see arrivals at the
 * end). Clicking the toast navigates to the comment's permalink, which the
 * virtualizer pages in and scrolls to.
 */
function useShowToastForNewComment(issueID: string) {
  const {userID} = useZero();
  const [latest, latestResult] = useQuery(
    queries.commentsPage({
      issueID,
      limit: 1,
      start: null,
      dir: 'backward',
      inclusive: true,
    }),
  );
  // The newest comment id at mount; only arrivals after that toast.
  const lastSeenID = useRef<string | null>(null);

  useEffect(() => {
    if (latestResult.type !== 'complete') {
      return;
    }
    const newest = latest?.[0];
    if (lastSeenID.current === null) {
      lastSeenID.current = newest?.id ?? '';
      return;
    }
    if (!newest || newest.id === lastSeenID.current) {
      return;
    }
    lastSeenID.current = newest.id;
    if (newest.creatorID === userID || !newest.creator) {
      return;
    }

    // No toast if the comment is already on screen.
    const el = document.querySelector(
      `[data-vrow-key="${CSS.escape(newest.id)}"]`,
    );
    if (el) {
      const rect = el.getBoundingClientRect();
      if (rect.bottom > 0 && rect.top < window.innerHeight) {
        return;
      }
    }

    toast(
      <ToastContent toastID={newest.id}>
        <AvatarImage className="toast-avatar-icon" user={newest.creator} />
        {newest.creator.login + ' posted a new comment'}
      </ToastContent>,
      {
        toastId: newest.id,
        containerId: 'bottom',
        onClick: () => {
          navigate(`#${makePermalink(newest)}`);
        },
      },
    );
  }, [latest, latestResult.type, userID]);
}
