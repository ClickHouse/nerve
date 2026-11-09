import { useEffect, useMemo, useRef, useState, type RefObject } from 'react';
import { api } from '../../api/client';
import type { MessageBlock } from '../../types/chat';
import type {
  PullRequestError, PullRequestResult, PullRequestReview, PullRequestState, PullRequestSummary,
} from '../../types/github';
import { findPullRequests, type PullRequestRef } from '../../utils/pullRequestLinks';
import { Badge, type BadgeTone } from '../ui/Badge';
import {
  Ban, Check, CircleDashed, ExternalLink, GitMerge, GitPullRequest, GitPullRequestClosed,
  GitPullRequestDraft, X, type Icon,
} from '../ui/icons';

/** How often a card of an open pull request with running checks reads it again. */
const REFRESH_MS = 60_000;

/** The chat end counts as in view up to this distance (px), as in `MessageList`. */
const NEAR_END_PX = 100;

const STATES: Record<PullRequestState, { label: string; tone: BadgeTone; icon: Icon; color: string }> = {
  draft: { label: 'Draft', tone: 'neutral', icon: GitPullRequestDraft, color: 'text-text-muted' },
  open: { label: 'Open', tone: 'success', icon: GitPullRequest, color: 'text-success' },
  merged: { label: 'Merged', tone: 'purple', icon: GitMerge, color: 'text-hue-violet' },
  closed: { label: 'Closed', tone: 'danger', icon: GitPullRequestClosed, color: 'text-error' },
};

const REVIEWS: Record<PullRequestReview, { label: string; color: string }> = {
  approved: { label: 'Approved', color: 'text-success' },
  changes_requested: { label: 'Changes requested', color: 'text-error' },
  review_required: { label: 'Review required', color: 'text-text-muted' },
};

const ERRORS: Record<PullRequestError, string> = {
  no_access: 'No access to details',
  gh_unavailable: 'GitHub CLI not available',
};

const LOAD_FAILED = 'Could not load details';

function isLive(result: PullRequestResult): boolean {
  const pull = result.pull;
  return !!pull && (pull.state === 'open' || pull.state === 'draft') && pull.checks.running > 0;
}

/** A card list this close (px) to the viewport counts as in view, so it reads just before it scrolls in. */
const VIEW_MARGIN = '200px 0px';

/** Whether `ref` is in or near the viewport. Without IntersectionObserver: always. */
function useInView(ref: RefObject<HTMLElement | null>): boolean {
  const [inView, setInView] = useState(typeof IntersectionObserver === 'undefined');
  useEffect(() => {
    const el = ref.current;
    if (!el || typeof IntersectionObserver === 'undefined') return;
    const observer = new IntersectionObserver(
      ([entry]) => setInView(entry.isIntersecting),
      { rootMargin: VIEW_MARGIN },
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [ref]);
  return inView;
}

/** Whether the browser tab shows. */
function useTabVisible(): boolean {
  const [visible, setVisible] = useState(() => !document.hidden);
  useEffect(() => {
    const onChange = () => setVisible(!document.hidden);
    document.addEventListener('visibilitychange', onChange);
    return () => document.removeEventListener('visibilitychange', onChange);
  }, []);
  return visible;
}

interface Loaded {
  key: string;
  results: Map<string, PullRequestResult> | null;
  failed: boolean;
  /** `Date.now()` when the last request ended. */
  at: number;
}

/**
 * The results for `urls`. They are read the first time `visible` is true,
 * then again `REFRESH_MS` after each read while one of them is open with
 * checks running and `visible` stays true. After a failed read, they are
 * read again `REFRESH_MS` later while `visible` is true. A failed refresh
 * keeps the last results.
 */
function usePullRequests(urls: string[], visible: boolean): { results: Map<string, PullRequestResult> | null; failed: boolean } {
  const key = urls.join('\n');
  const [loaded, setLoaded] = useState<Loaded | null>(null);
  const current = loaded?.key === key ? loaded : null;

  useEffect(() => {
    if (!visible) return;
    const results = current?.results ?? null;
    const live = !!results && [...results.values()].some(isLive);
    if (current && !live && !current.failed) return;
    const wait = current ? Math.max(0, current.at + REFRESH_MS - Date.now()) : 0;
    const timer = setTimeout(() => {
      void api.getPullRequests(key.split('\n')).then(
        ({ pulls }) => setLoaded({ key, results: new Map(pulls.map(p => [p.url, p])), failed: false, at: Date.now() }),
        () => setLoaded({ key, results, failed: true, at: Date.now() }),
      );
    }, wait);
    return () => clearTimeout(timer);
  }, [key, visible, current]);

  return { results: current?.results ?? null, failed: current?.failed ?? false };
}

function Checks({ pull }: { pull: PullRequestSummary }) {
  const { passed, failed, running, cancelled } = pull.checks;
  const review = pull.review ? REVIEWS[pull.review] : null;
  if (!passed && !failed && !running && !cancelled && !review) return null;
  return (
    <div className="mt-1.5 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs">
      {failed > 0 && (
        <span className="inline-flex items-center gap-1 text-error">
          <X size={12} aria-hidden /> {failed} failed
        </span>
      )}
      {cancelled > 0 && (
        <span className="inline-flex items-center gap-1 text-text-muted">
          <Ban size={12} aria-hidden /> {cancelled} cancelled
        </span>
      )}
      {running > 0 && (
        <span className="inline-flex items-center gap-1 text-warning">
          <CircleDashed size={12} aria-hidden /> {running} running
        </span>
      )}
      {passed > 0 && (
        <span className="inline-flex items-center gap-1 text-success">
          <Check size={12} aria-hidden /> {passed} passed
        </span>
      )}
      {review && <span className={review.color}>{review.label}</span>}
    </div>
  );
}

function PullRequestCard({ pr, result, failed }: {
  pr: PullRequestRef;
  result: PullRequestResult | undefined;
  failed: boolean;
}) {
  const pull = result?.pull ?? null;
  const state = pull ? STATES[pull.state] : null;
  const StateIcon = state?.icon ?? GitPullRequest;
  const reference = `${pr.repo}#${pr.number}`;
  let detail: string;
  if (pull) {
    detail = pull.head_ref && pull.base_ref
      ? `${reference} · ${pull.head_ref} → ${pull.base_ref}`
      : reference;
  } else if (result?.error) {
    detail = ERRORS[result.error];
  } else {
    detail = failed ? LOAD_FAILED : 'Loading details…';
  }

  return (
    <li
      className="max-w-xl rounded-lg border border-border bg-surface px-3 py-2.5"
      data-pr-state={pull?.state ?? result?.error ?? (failed ? 'failed' : 'loading')}
    >
      <div className="flex items-start gap-2.5">
        <StateIcon size={16} aria-hidden className={`shrink-0 mt-0.5 ${state?.color ?? 'text-text-faint'}`} />
        <div className="min-w-0 flex-1">
          <div className="flex items-start gap-2">
            <a
              href={pr.url}
              target="_blank"
              rel="noopener noreferrer"
              title="Open on GitHub"
              className="group/pr min-w-0 line-clamp-2 break-words rounded-sm text-sm font-medium leading-snug
                         text-text no-underline hover:text-accent"
            >
              {pull?.title || reference}
              <ExternalLink
                size={12}
                aria-hidden
                className="ml-1 inline-block align-baseline text-text-faint group-hover/pr:text-accent"
              />
            </a>
            {state && <Badge tone={state.tone} pill className="ml-auto shrink-0">{state.label}</Badge>}
          </div>
          <div className="mt-0.5 truncate text-xs text-text-muted">{detail}</div>
          {pull && <Checks pull={pull} />}
        </div>
      </div>
    </li>
  );
}

/** The nearest ancestor that scrolls vertically. */
function scrollParent(el: HTMLElement): HTMLElement | null {
  for (let node = el.parentElement; node; node = node.parentElement) {
    if (/auto|scroll/.test(getComputedStyle(node).overflowY)) return node;
  }
  return null;
}

function PullRequestCardList({ prs }: { prs: PullRequestRef[] }) {
  const listRef = useRef<HTMLUListElement>(null);
  const inView = useInView(listRef);
  const tabVisible = useTabVisible();
  const { results, failed } = usePullRequests(prs.map(pr => pr.url), inView && tabVisible);

  // The details arrive after the chat scrolled to its end and make the cards
  // taller. While the end of the chat is in view, keep it in view.
  useEffect(() => {
    const list = listRef.current;
    if (!list || typeof ResizeObserver === 'undefined') return;
    let height: number | null = null;
    const observer = new ResizeObserver(([entry]) => {
      const growth = height === null ? 0 : entry.contentRect.height - height;
      height = entry.contentRect.height;
      const scroller = growth > 0 ? scrollParent(list) : null;
      if (scroller && scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight - growth < NEAR_END_PX) {
        scroller.scrollTop = scroller.scrollHeight;
      }
    });
    observer.observe(list);
    return () => observer.disconnect();
  }, []);

  return (
    <ul ref={listRef} aria-label="Pull requests" className="mt-3 flex flex-col gap-2">
      {prs.map(pr => (
        <PullRequestCard key={pr.url} pr={pr} result={results?.get(pr.url)} failed={failed} />
      ))}
    </ul>
  );
}

/** One card per GitHub pull request that the message links to. */
export function PullRequestCards({ blocks }: { blocks: MessageBlock[] }) {
  const prs = useMemo(() => findPullRequests(blocks), [blocks]);
  if (prs.length === 0) return null;
  return <PullRequestCardList prs={prs} />;
}
