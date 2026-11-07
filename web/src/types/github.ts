// `GET /api/github/pulls`: pull request summaries for the PR cards.

export type PullRequestState = 'draft' | 'open' | 'merged' | 'closed';

export type PullRequestReview = 'approved' | 'changes_requested' | 'review_required';

/** `no_access`: gh could not read the pull request (no login, no permission,
 *  no such pull request). `gh_unavailable`: gh is missing or did not answer. */
export type PullRequestError = 'no_access' | 'gh_unavailable';

export interface PullRequestChecks {
  passed: number;
  failed: number;
  running: number;
}

export interface PullRequestSummary {
  /** `owner/repo` */
  repo: string;
  number: number;
  title: string;
  state: PullRequestState;
  review: PullRequestReview | null;
  checks: PullRequestChecks;
  head_ref: string | null;
  base_ref: string | null;
}

/** One result per requested URL: `pull` or `error`, never both. */
export interface PullRequestResult {
  url: string;
  pull: PullRequestSummary | null;
  error: PullRequestError | null;
}
