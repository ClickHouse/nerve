import type { MessageBlock } from '../types/chat';

/** Most PR cards under one message. */
export const MAX_PR_CARDS = 5;

/**
 * The same owner, repo and number rules as the server's `PULL_URL`. After the
 * number comes the end of the text, a path (`/files`), a query, a fragment or
 * punctuation. A letter, a digit or a hyphen after it is not a pull request URL.
 */
const PULL_URL =
  /https:\/\/github\.com\/([A-Za-z0-9][A-Za-z0-9-]{0,38})\/((?!\.+\/)[A-Za-z0-9._-]{1,100})\/pull\/([1-9]\d{0,9})(?![A-Za-z0-9-])/g;

export interface PullRequestRef {
  /** `https://github.com/<owner>/<repo>/pull/<number>`, nothing after it. */
  url: string;
  /** `owner/repo` */
  repo: string;
  number: number;
}

/**
 * The GitHub pull requests that a message links to, at most `limit`, each one
 * once. They come from its text. Only when the text links to none, they come
 * from each tool result that links to exactly one pull request, for example
 * the URL that `gh pr create` prints. The output of a command that lists pull
 * requests thus gives no cards.
 */
export function findPullRequests(blocks: MessageBlock[], limit = MAX_PR_CARDS): PullRequestRef[] {
  const inText = scan(blocks.flatMap(b => (b.type === 'text' ? [b.content] : [])), limit);
  if (inText.length > 0) return inText;
  const single = blocks.flatMap(b => {
    const prs = b.type === 'tool_call' && b.result ? scan([b.result], 2) : [];
    return prs.length === 1 ? [prs[0].url] : [];
  });
  return scan(single, limit);
}

/** The pull requests in `texts`, in order of first link, at most `limit`, each one once. */
function scan(texts: string[], limit: number): PullRequestRef[] {
  const found = new Map<string, PullRequestRef>();
  for (const text of texts) {
    for (const [, owner, repo, number] of text.matchAll(PULL_URL)) {
      const url = `https://github.com/${owner}/${repo}/pull/${number}`;
      const key = url.toLowerCase();
      if (found.has(key)) continue;
      found.set(key, { url, repo: `${owner}/${repo}`, number: Number(number) });
      if (found.size === limit) return [...found.values()];
    }
  }
  return [...found.values()];
}
