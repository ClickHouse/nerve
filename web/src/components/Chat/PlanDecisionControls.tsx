import { useId, useRef, useState, type FormEvent, type KeyboardEvent } from 'react';
import { BookOpen, Check, MessageSquare, Pencil, Send, X } from '../ui/icons';
import { Button, TextArea } from '../ui';
import {
  PLAN_APPLY_EDITED, PLAN_EDIT, PLAN_EDIT_CANCEL, PLAN_FEEDBACK_LABEL, PLAN_FEEDBACK_SEND, PLAN_READ,
  usePlanDecisionCopy,
} from './planDecision';

/**
 * The plan decision's controls: the one implementation behind the plan card
 * in the chat and the plan panel's footer (labels from `planDecision.ts`).
 *
 * - **Approve**.
 * - **Request changes…** opens a feedback field. It must hold some text
 *   before Send sends it. Cmd/Ctrl+Enter sends; Escape closes the field and
 *   keeps what was typed.
 * - In the card, **Read the plan** and **Edit** open the plan in the side
 *   panel (Edit with the footer's editor open); the card shows no plan text.
 * - In the footer, **Edit** (given the plan and `onApproveEdited`) turns the
 *   plan into a text field above the buttons. Approve and Request changes
 *   give way to **Apply edited plan**, enabled once the text differs from
 *   the plan and is not empty, and **Cancel**, which drops the edits.
 *   Cmd/Ctrl+Enter applies; Escape cancels only while nothing was changed,
 *   so a stray key loses no work. An Edit asked for from the card
 *   (`editRequest`) opens the editor, once per request.
 *
 * Escape never leaves these fields: the app-wide Escape stops a running
 * turn, and the turn is running — it waits for this very decision.
 */
export function PlanDecisionControls({
  layout, onApprove, onRequestChanges, onReadPlan, onEdit, plan, onApproveEdited, editRequest,
}: {
  /**
   * `card`: the buttons in a row, the feedback field below them.
   * `footer`: compact buttons on the right, any field above them.
   * Either way the buttons stay where they are when a field opens.
   */
  layout: 'card' | 'footer';
  onApprove: () => void;
  /** Receives the trimmed feedback. Omitted: no Request changes is offered. */
  onRequestChanges?: (feedback: string) => void;
  /** Card: show the plan in the side panel. */
  onReadPlan?: () => void;
  /** Card: show the plan in the side panel with its editor open. */
  onEdit?: () => void;
  /** Footer: the plan as markdown, the text the editor starts from. */
  plan?: string | null;
  /** Footer: receives the edited plan. Omitted (or no plan): no Edit is offered. */
  onApproveEdited?: (plan: string) => void;
  /** Footer: the id of an Edit asked for from the card. */
  editRequest?: string | null;
}) {
  const copy = usePlanDecisionCopy();
  const [feedbackOpen, setFeedbackOpen] = useState(false);
  const [feedback, setFeedback] = useState('');
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState('');
  const formId = useId();
  const fieldId = useId();
  const editorId = useId();
  const toggleRef = useRef<HTMLButtonElement>(null);
  const editRef = useRef<HTMLButtonElement>(null);
  const card = layout === 'card';
  const canSend = feedback.trim().length > 0;
  const canEdit = !card && !!plan && !!onApproveEdited;
  const canApplyEdit = canEdit && draft.trim().length > 0 && draft.trim() !== plan.trim();

  // An Edit asked for from the card opens the editor here, once per request.
  // Set during render, React's pattern for acting on a changed prop.
  const [takenRequest, setTakenRequest] = useState<string | null>(null);
  if (editRequest && editRequest !== takenRequest) {
    setTakenRequest(editRequest);
    if (canEdit) {
      setDraft(plan);
      setFeedbackOpen(false);
      setEditing(true);
    }
  }

  const send = () => {
    if (canSend) onRequestChanges?.(feedback.trim());
  };

  const onSubmit = (e: FormEvent) => {
    e.preventDefault();
    send();
  };

  const onFeedbackKey = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      send();
    } else if (e.key === 'Escape') {
      e.preventDefault();
      e.stopPropagation();
      setFeedbackOpen(false);
      toggleRef.current?.focus();
    }
  };

  const startEdit = () => {
    if (!plan) return;
    setDraft(plan);
    setFeedbackOpen(false);
    setEditing(true);
  };

  const cancelEdit = () => {
    setEditing(false);
    // The editor goes away with the focus in it; hand the focus back to Edit
    // once it is rendered again.
    requestAnimationFrame(() => editRef.current?.focus());
  };

  const applyEdit = () => {
    if (canApplyEdit) onApproveEdited?.(draft);
  };

  const onEditorKey = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      applyEdit();
    } else if (e.key === 'Escape') {
      e.preventDefault();
      e.stopPropagation();
      if (draft.trim() === plan?.trim()) cancelEdit();
    }
  };

  const size = card ? 'md' : 'sm';
  const iconSize = card ? 13 : 12;
  const secondaryClass = card ? '' : 'py-1 rounded-md font-medium';

  const approve = (
    <Button variant={card ? 'primary' : 'success'} size={size} className={card ? 'flex-1' : 'py-1'} onClick={onApprove}>
      <Check size={iconSize} />
      {copy.approve}
    </Button>
  );

  const requestChanges = onRequestChanges && (
    <Button
      ref={toggleRef}
      variant="secondary"
      size={size}
      className={card ? 'flex-1' : secondaryClass}
      aria-expanded={feedbackOpen}
      aria-controls={feedbackOpen ? formId : undefined}
      onClick={() => setFeedbackOpen(o => !o)}
    >
      <MessageSquare size={iconSize} />
      {copy.requestChanges}
    </Button>
  );

  const field = !editing && feedbackOpen && onRequestChanges && (
    <form id={formId} onSubmit={onSubmit} className="space-y-1.5">
      <label htmlFor={fieldId} className="block text-xs font-medium text-text-secondary">
        {PLAN_FEEDBACK_LABEL}
      </label>
      <TextArea
        id={fieldId}
        value={feedback}
        onChange={e => setFeedback(e.target.value)}
        onKeyDown={onFeedbackKey}
        required
        rows={3}
        resizable
        // Opened on purpose a moment ago, by the button above or beside it.
        autoFocus
        className="max-h-60"
      />
      <div className="flex justify-end">
        <Button type="submit" variant="primary" size="sm" disabled={!canSend}>
          <Send size={12} />
          {PLAN_FEEDBACK_SEND}
        </Button>
      </div>
    </form>
  );

  if (card) {
    return (
      <div className="space-y-3">
        <div className="flex flex-wrap gap-2">
          {approve}
          {requestChanges}
          {onReadPlan && (
            <Button variant="ghost" size="md" onClick={onReadPlan}>
              <BookOpen size={13} />
              {PLAN_READ}
            </Button>
          )}
          {onEdit && (
            <Button variant="ghost" size="md" onClick={onEdit}>
              <Pencil size={13} />
              {PLAN_EDIT}
            </Button>
          )}
        </div>
        {field}
      </div>
    );
  }

  const editor = editing && (
    <div className="space-y-1.5">
      <label htmlFor={editorId} className="block text-xs font-medium text-text-secondary">{copy.editLabel}</label>
      <TextArea
        id={editorId}
        value={draft}
        onChange={e => setDraft(e.target.value)}
        onKeyDown={onEditorKey}
        rows={10}
        resizable
        // Opened on purpose a moment ago, by Edit.
        autoFocus
        spellCheck={false}
        className="font-mono text-xs max-h-[40vh]"
      />
    </div>
  );

  return (
    <div className="space-y-2">
      {editor}
      {field}
      <div className="flex items-center justify-end gap-2">
        {editing ? (
          <>
            <Button variant="secondary" size="sm" className={secondaryClass} onClick={cancelEdit}>
              <X size={12} />
              {PLAN_EDIT_CANCEL}
            </Button>
            <Button variant="success" size="sm" className="py-1" disabled={!canApplyEdit} onClick={applyEdit}>
              <Check size={12} />
              {PLAN_APPLY_EDITED}
            </Button>
          </>
        ) : (
          <>
            {canEdit && (
              <Button ref={editRef} variant="secondary" size="sm" className={secondaryClass} onClick={startEdit}>
                <Pencil size={12} />
                {PLAN_EDIT}
              </Button>
            )}
            {requestChanges}
            {approve}
          </>
        )}
      </div>
    </div>
  );
}
