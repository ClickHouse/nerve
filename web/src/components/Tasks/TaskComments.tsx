import { useCallback, useEffect, useState } from 'react';
import { api, type TaskComment } from '../../api/client';
import { useAuthStore } from '../../stores/authStore';
import { formatTimeAgo } from '../../utils/dateGroups';
import { ActorAttribution } from '../Chat/ActorLabel';
import { MarkdownContent } from '../Chat/MarkdownContent';
import { Button, IconButton, TextArea } from '../ui';
import { Trash2 } from '../ui/icons';

/** The server refuses a longer body. */
const MAX_COMMENT_LENGTH = 10_000;

/**
 * The comments of a task, oldest first, and the box to add one.
 *
 * `refreshKey` is the task's `updated_at`. Adding or deleting a comment
 * changes it, and the task broadcast carries the new value, so a comment by
 * another person or by the agent shows while the task is open.
 *
 * Only the author of a comment can delete it, so the delete button shows on
 * the viewer's own comments only.
 */
export function TaskComments({ taskId, refreshKey }: { taskId: string; refreshKey?: string }) {
  const viewerId = useAuthStore((s) => s.viewer?.id ?? null);
  const [comments, setComments] = useState<TaskComment[] | null>(null);
  const [draft, setDraft] = useState('');
  const [posting, setPosting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    api.listTaskComments(taskId)
      .then(({ comments }) => { if (!cancelled) setComments(comments); })
      // Keep what is shown; an empty list only when nothing loaded yet.
      .catch(() => { if (!cancelled) setComments((shown) => shown ?? []); });
    return () => { cancelled = true; };
  }, [taskId, refreshKey]);

  const submit = useCallback(async () => {
    const body = draft.trim();
    if (!body || posting) return;
    setPosting(true);
    setError(null);
    try {
      const { comment } = await api.addTaskComment(taskId, body);
      // The refetch that the broadcast starts can arrive first.
      setComments((shown) =>
        shown?.some((c) => c.id === comment.id) ? shown : [...(shown ?? []), comment]);
      setDraft('');
    } catch {
      setError('The comment was not added. Try again.');
    } finally {
      setPosting(false);
    }
  }, [draft, posting, taskId]);

  const remove = async (comment: TaskComment) => {
    if (!window.confirm('Delete this comment?')) return;
    setError(null);
    try {
      await api.deleteTaskComment(taskId, comment.id);
      setComments((shown) => shown?.filter((c) => c.id !== comment.id) ?? null);
    } catch {
      setError('The comment was not deleted. Try again.');
    }
  };

  return (
    <section aria-label="Comments" className="mt-8 pt-5 border-t border-border-subtle">
      <h2 className="text-sm font-semibold text-text mb-3">
        Comments{comments?.length ? ` (${comments.length})` : ''}
      </h2>

      {comments === null && <p className="text-xs text-text-faint mb-4">Loading comments...</p>}
      {comments?.length === 0 && <p className="text-xs text-text-faint mb-4">No comments yet.</p>}
      {!!comments?.length && (
        <ol className="space-y-4 mb-5">
          {comments.map((comment) => (
            <li key={comment.id}>
              <div className="flex items-center gap-2 text-xs mb-1 min-w-0">
                <span className="font-medium text-text-muted min-w-0 truncate">
                  {comment.actor_id ? (
                    <ActorAttribution
                      actorId={comment.actor_id}
                      causeId={comment.caused_by_actor_id}
                      verb="Written by"
                    />
                  ) : 'Unknown'}
                </span>
                <span className="text-text-faint shrink-0" title={comment.created_at}>
                  {formatTimeAgo(comment.created_at)}
                </span>
                {viewerId !== null && comment.actor_id === viewerId && (
                  <IconButton
                    label="Delete comment"
                    size="xs"
                    variant="dangerGhost"
                    className="ml-auto"
                    onClick={() => void remove(comment)}
                  >
                    <Trash2 size={12} />
                  </IconButton>
                )}
              </div>
              <MarkdownContent content={comment.body} />
            </li>
          ))}
        </ol>
      )}

      <TextArea
        aria-label="New comment"
        placeholder="Add a comment..."
        value={draft}
        maxLength={MAX_COMMENT_LENGTH}
        onChange={(e) => { setDraft(e.target.value); setError(null); }}
        onKeyDown={(e) => {
          if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
            e.preventDefault();
            void submit();
          }
        }}
      />
      <div className="flex items-center gap-3 mt-2">
        {error && <span role="alert" className="text-xs text-error">{error}</span>}
        <Button
          variant="primary"
          size="xs"
          className="ml-auto"
          onClick={() => void submit()}
          disabled={!draft.trim() || posting}
        >
          {posting ? 'Adding...' : 'Comment'}
        </Button>
      </div>
    </section>
  );
}
