// Code-review panel types — mirror the backend shapes in
// nerve/gateway/routes/reviews.py and nerve/db/reviews.py.

export interface ReviewWorktree {
  path: string;
  branch?: string;
  head?: string;
}

export interface ReviewRepo {
  root: string;
  resolved: string;
  worktrees: ReviewWorktree[];
}

export interface ReviewChangedFile {
  path: string;
  status: 'created' | 'modified' | 'deleted' | 'renamed';
  additions: number;
  deletions: number;
}

export interface ReviewComment {
  id: string;
  thread_id: string;
  author: 'human' | 'agent';
  body: string;
  created_at: string;
  /** 1 = staged draft (not yet delivered to the session); 0 = submitted. */
  pending?: number;
}

export interface ReviewThread {
  id: string;
  review_id: string;
  file_path: string;
  side: 'new' | 'old';
  line_start: number | null;
  line_end: number | null;
  anchor_snippet: string | null;
  status: 'open' | 'answered' | 'resolved';
  created_at: string;
  updated_at: string;
  comments?: ReviewComment[];
}

export interface Review {
  id: string;
  title: string;
  repo_root: string;
  worktree: string;
  branch: string | null;
  base_ref: string;
  target_session_id: string | null;
  created_by: 'human' | 'agent';
  status: 'open' | 'resolved';
  created_at: string;
  updated_at: string;
  thread_count?: number;
  open_thread_count?: number;
  threads?: ReviewThread[];
}
