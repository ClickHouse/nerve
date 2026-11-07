import { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../../api/client';
import type { MessageBlock } from '../../types/chat';
import type {
  PullRequestError, PullRequestResult, PullRequestReview, PullRequestState,
} from '../../types/github';
import { findPullRequests, type PullRequestRef } from '../../utils/pullRequestLinks';
import { Badge, type BadgeTone } from '../ui/Badge';
import {
  Check, CircleDashed, ExternalLink, GitMerge, GitPullRequest, GitPullRequestClosed,
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

/**
 * The results for `urls`, read once, then again every `REFRESH_MS` while one
 * of them is open with checks running. A hidden tab skips a refresh.
 */
function usePullRequests(urls: string[]): { results: Map<string, PullRequestResult> | null; failed: boolean } {
  const key = urls.join('\n');
  const [results, setResults] = useState<Map<string, PullRequestResult> | null>(null);
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    let stopped = false;
    let loaded = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const refreshLater = () => { timer = setTimeout(refresh, REFRESH_MS); };
    const load = async () => {
      try {
        const { pulls } = await api.getPullRequests(key.split('\n'));
        if (stopped) return;
        loaded = true;
        setResults(new Map(pulls.map(p => [p.url, p])));
        if (pulls.some(isLive)) refreshLater();
      } catch {
        if (stopped) return;
        // A failed refresh keeps the cards as they are and tries again.
        if (loaded) refreshLater();
        else setFailed(true);
      }
    };
    function refresh() {
      if (document.hidden) refreshLater();
      else void load();
    }
    void load();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }, [key]);

  return { results, failed };
}

function Checks({ result }: { result: PullRequestResult }) {
  const pull = result.pull!;
  const { passed, failed, running } = pull.checks;
  const review = pull.review ? REVIEWS[pull.review] : null;
  if (!passed && !failed && !running && !review) return null;
  return (
    <div className="mt-1.5 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs">
      {failed > 0 && (
        <span className="inline-flex items-center gap-1 text-error">
          <X size={12} aria-hidden /> {failed} failed
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
          {pull && <Checks result={result!} />}
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
  const { results, failed } = usePullRequests(prs.map(pr => pr.url));
  const listRef = useRef<HTMLUListElement>(null);

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
