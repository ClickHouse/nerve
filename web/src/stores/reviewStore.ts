import { create } from 'zustand';
import { api } from '../api/client';
import type { FileDiff } from '../types/chat';
import type { Review, ReviewChangedFile, ReviewRepo } from '../types/review';

export interface CommentAnchor {
  file_path: string;
  side: 'new' | 'old';
  line_start: number | null;
  line_end: number | null;
  anchor_snippet: string | null;
}

interface SessionOption {
  id: string;
  title: string;
  is_running: boolean;
}

interface ReviewState {
  repos: ReviewRepo[];
  worktree: string | null;
  branch: string | null;
  base: string;
  changed: ReviewChangedFile[];
  path: string | null;
  mode: 'diff' | 'file';
  diff: FileDiff | null;
  fileContent: { content: string | null; binary: boolean; too_large: boolean } | null;
  review: Review | null;          // active review (with threads) for this worktree
  reviews: Review[];              // all reviews (sidebar)
  sessions: SessionOption[];      // running sessions for the target picker
  target: string;                 // chosen target session id ('' = mint on first comment)
  loadingRepos: boolean;
  loadingChanged: boolean;
  loadingDiff: boolean;
  busy: boolean;                  // a mutation (comment/reply/resolve) is in flight
  error: string | null;

  loadRepos: () => Promise<void>;
  loadSessions: () => Promise<void>;
  loadReviews: () => Promise<void>;
  selectWorktree: (worktree: string, branch: string | null) => Promise<void>;
  setBase: (base: string) => Promise<void>;
  selectFile: (path: string) => Promise<void>;
  openFullFile: (path: string) => Promise<void>;
  selectReview: (review: Review) => Promise<void>;
  refreshReview: () => Promise<void>;
  setTarget: (sessionId: string) => Promise<void>;
  addComment: (anchor: CommentAnchor, body: string) => Promise<void>;
  reply: (threadId: string, body: string) => Promise<void>;
  resolve: (threadId: string) => Promise<void>;
  clearError: () => void;
}

function errMsg(e: unknown, fallback: string): string {
  const raw = e instanceof Error ? e.message : String(e);
  const m = raw.match(/^\d+:\s*(.*)$/s);
  const body = m ? m[1] : raw;
  try {
    const parsed = JSON.parse(body);
    if (parsed && typeof parsed.detail === 'string') return parsed.detail;
  } catch { /* not JSON */ }
  return body || fallback;
}

export const useReviewStore = create<ReviewState>((set, get) => ({
  repos: [],
  worktree: null,
  branch: null,
  base: 'HEAD',
  changed: [],
  path: null,
  mode: 'diff',
  diff: null,
  fileContent: null,
  review: null,
  reviews: [],
  sessions: [],
  target: '',
  loadingRepos: true,
  loadingChanged: false,
  loadingDiff: false,
  busy: false,
  error: null,

  loadRepos: async () => {
    set({ loadingRepos: true });
    try {
      const { repos } = await api.reviewRepos();
      set({ repos, loadingRepos: false });
    } catch (e) {
      set({ loadingRepos: false, error: errMsg(e, 'Failed to load repos') });
    }
  },

  loadSessions: async () => {
    try {
      const { sessions } = await api.listSessions();
      set({
        sessions: (sessions || [])
          .filter((s: any) => s.is_running)
          .map((s: any) => ({ id: s.id, title: s.title || s.id, is_running: !!s.is_running })),
      });
    } catch { /* non-fatal */ }
  },

  loadReviews: async () => {
    try {
      const { reviews } = await api.listReviews();
      set({ reviews });
    } catch (e) {
      set({ error: errMsg(e, 'Failed to load reviews') });
    }
  },

  selectWorktree: async (worktree, branch) => {
    set({
      worktree, branch, path: null, diff: null, fileContent: null,
      review: null, mode: 'diff', changed: [], loadingChanged: true,
    });
    await get().loadReviews();
    // Auto-attach the most recent open review for this worktree, if any.
    const existing = get().reviews.find(r => r.worktree === worktree && r.status === 'open');
    if (existing) {
      try {
        const full = await api.getReview(existing.id);
        set({ review: full, target: full.target_session_id || '' });
      } catch { /* ignore */ }
    }
    try {
      const { files } = await api.reviewChanged(worktree, get().base);
      set({ changed: files, loadingChanged: false });
    } catch (e) {
      set({ loadingChanged: false, error: errMsg(e, 'Failed to list changes') });
    }
  },

  setBase: async (base) => {
    set({ base });
    const wt = get().worktree;
    if (wt) await get().selectWorktree(wt, get().branch);
  },

  selectFile: async (path) => {
    const wt = get().worktree;
    if (!wt) return;
    set({ path, mode: 'diff', diff: null, fileContent: null, loadingDiff: true });
    try {
      const diff = await api.reviewDiff(wt, path, get().base);
      set({ diff, loadingDiff: false });
    } catch (e) {
      set({ loadingDiff: false, error: errMsg(e, 'Failed to load diff') });
    }
  },

  openFullFile: async (path) => {
    const wt = get().worktree;
    if (!wt) return;
    set({ path, mode: 'file', diff: null, fileContent: null, loadingDiff: true });
    try {
      const fileContent = await api.reviewFile(wt, path);
      set({ fileContent, loadingDiff: false });
    } catch (e) {
      set({ loadingDiff: false, error: errMsg(e, 'Failed to load file') });
    }
  },

  selectReview: async (review) => {
    set({
      worktree: review.worktree, branch: review.branch, base: review.base_ref,
      path: null, diff: null, fileContent: null, mode: 'diff',
      changed: [], loadingChanged: true, target: review.target_session_id || '',
    });
    try {
      const full = await api.getReview(review.id);
      set({ review: full });
      const firstThread = full.threads && full.threads[0];
      try {
        const { files } = await api.reviewChanged(review.worktree, review.base_ref);
        set({ changed: files, loadingChanged: false });
      } catch {
        set({ loadingChanged: false });
      }
      if (firstThread) await get().selectFile(firstThread.file_path);
    } catch (e) {
      set({ loadingChanged: false, error: errMsg(e, 'Failed to open review') });
    }
  },

  refreshReview: async () => {
    const r = get().review;
    if (!r) return;
    try {
      const full = await api.getReview(r.id);
      set({ review: full });
    } catch { /* ignore */ }
  },

  setTarget: async (sessionId) => {
    set({ target: sessionId });
    const r = get().review;
    if (r) {
      try {
        await api.patchReview(r.id, { target_session_id: sessionId || null });
        await get().refreshReview();
      } catch (e) {
        set({ error: errMsg(e, 'Failed to set target session') });
      }
    }
  },

  addComment: async (anchor, body) => {
    const wt = get().worktree;
    if (!wt) return;
    set({ busy: true, error: null });
    try {
      let review = get().review;
      if (!review) {
        review = await api.createReview({
          worktree: wt,
          branch: get().branch,
          base_ref: get().base,
          target_session_id: get().target || null,
          created_by: 'human',
          title: `Review of ${get().branch || wt}`,
        });
        set({ review });
        await get().loadReviews();
      }
      const res = await api.addReviewThread(review.id, {
        file_path: anchor.file_path,
        side: anchor.side,
        line_start: anchor.line_start,
        line_end: anchor.line_end,
        anchor_snippet: anchor.anchor_snippet,
        body,
        author: 'human',
      });
      if (res.target_session_id) set({ target: res.target_session_id });
      await get().refreshReview();
    } catch (e) {
      set({ error: errMsg(e, 'Failed to add comment') });
    } finally {
      set({ busy: false });
    }
  },

  reply: async (threadId, body) => {
    const r = get().review;
    if (!r) return;
    set({ busy: true, error: null });
    try {
      await api.addReviewComment(r.id, threadId, { body, author: 'human' });
      await get().refreshReview();
    } catch (e) {
      set({ error: errMsg(e, 'Failed to reply') });
    } finally {
      set({ busy: false });
    }
  },

  resolve: async (threadId) => {
    const r = get().review;
    if (!r) return;
    set({ busy: true });
    try {
      await api.resolveReviewThread(r.id, threadId);
      await get().refreshReview();
    } catch (e) {
      set({ error: errMsg(e, 'Failed to resolve') });
    } finally {
      set({ busy: false });
    }
  },

  clearError: () => set({ error: null }),
}));
