import { useId, useRef, useState, type FormEvent, type KeyboardEvent, type Ref } from 'react';
import { Check, ChevronDown, ChevronUp, MessageSquare, Pencil, Send, X } from '../ui/icons';
import { Button, TextArea } from '../ui';
import { MarkdownContent } from './MarkdownContent';
import {
  PLAN_APPLY_EDITED, PLAN_EDIT, PLAN_EDIT_CANCEL, PLAN_FEEDBACK_LABEL, PLAN_FEEDBACK_SEND, usePlanDecisionCopy,
} from './planDecision';

/**
 * The plan decision's controls: the one implementation behind the plan card
 * in the chat and the plan panel's footer (labels from `planDecision.ts`).
 *
 * - **Approve**.
 * - **Request changes…** opens a feedback field. It must hold some text
 *   before Send sends it. Cmd/Ctrl+Enter sends; Escape closes the field and
 *   keeps what was typed.
 * - **Edit** (given the plan and `onApproveEdited`) turns the plan into a
 *   text field. Approve and Request changes give way to **Apply edited
 *   plan**, enabled once the text differs from the plan and is not empty,
 *   and **Cancel**, which drops the edits. Cmd/Ctrl+Enter applies; Escape
 *   cancels only while nothing was changed, so a stray key loses no work.
 *
 * In the card the plan itself is shown here too, as markdown in a box of at
 * most 24rem with its own scroll (or all of it), with Show all and Edit
 * under it; editing replaces the box. The panel shows the plan in its own
 * body, so the footer has an Edit button instead and puts the field above
 * its buttons.
 *
 * Escape never leaves these fields: the app-wide Escape stops a running
 * turn, and the turn is running — it waits for this very decision.
 */
export function PlanDecisionControls({ layout, onApprove, onRequestChanges, plan, onApproveEdited }: {
  /**
   * `card`: the plan, then two equal buttons, then the feedback field.
   * `footer`: compact buttons on the right, any field above them.
   * Either way the buttons stay where they are when a field opens.
   */
  layout: 'card' | 'footer';
  onApprove: () => void;
  /** Receives the trimmed feedback. Omitted: no Request changes is offered. */
  onRequestChanges?: (feedback: string) => void;
  /** The plan as markdown: shown in the card, and the text Edit starts from. */
  plan?: string | null;
  /** Receives the edited plan. Omitted (or no plan): no Edit is offered. */
  onApproveEdited?: (plan: string) => void;
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
  const canEdit = !!plan && !!onApproveEdited;
  const canApplyEdit = canEdit && draft.trim().length > 0 && draft.trim() !== plan.trim();

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
  const secondaryClass = card ? 'flex-1' : 'py-1 rounded-md font-medium';

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
      className={secondaryClass}
      aria-expanded={feedbackOpen}
      aria-controls={feedbackOpen ? formId : undefined}
      onClick={() => setFeedbackOpen(o => !o)}
    >
      <MessageSquare size={iconSize} />
      {copy.requestChanges}
    </Button>
  );

  const applyEdited = (
    <Button
      variant={card ? 'primary' : 'success'}
      size={size}
      className={card ? 'flex-1' : 'py-1'}
      disabled={!canApplyEdit}
      onClick={applyEdit}
    >
      <Check size={iconSize} />
      {PLAN_APPLY_EDITED}
    </Button>
  );

  const cancel = (
    <Button variant="secondary" size={size} className={secondaryClass} onClick={cancelEdit}>
      <X size={iconSize} />
      {PLAN_EDIT_CANCEL}
    </Button>
  );

  const editor = editing && (
    <div className="space-y-1.5">
      <label htmlFor={editorId} className="block text-xs font-medium text-text-secondary">{copy.editLabel}</label>
      <TextArea
        id={editorId}
        value={draft}
        onChange={e => setDraft(e.target.value)}
        onKeyDown={onEditorKey}
        rows={card ? 16 : 10}
        resizable
        // Opened on purpose a moment ago, by Edit.
        autoFocus
        spellCheck={false}
        className={`font-mono text-xs ${card ? 'max-h-96' : 'max-h-[40vh]'}`}
      />
    </div>
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
        {plan && (editing ? editor : (
          <PlanView text={plan} label={copy.planLabel} onEdit={canEdit ? startEdit : undefined} editRef={editRef} />
        ))}
        <div className="flex gap-2">
          {editing ? <>{applyEdited}{cancel}</> : <>{approve}{requestChanges}</>}
        </div>
        {field}
      </div>
    );
  }
  return (
    <div className="space-y-2">
      {editor}
      {field}
      <div className="flex items-center justify-end gap-2">
        {editing ? <>{cancel}{applyEdited}</> : (
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

/** The plan as markdown, in a box of at most 24rem with its own scroll, or all of it. */
function PlanView({ text, label, onEdit, editRef }: {
  text: string;
  label: string;
  onEdit?: () => void;
  editRef: Ref<HTMLButtonElement>;
}) {
  const [expanded, setExpanded] = useState(false);
  const regionId = useId();
  return (
    <div>
      <div
        id={regionId}
        role="region"
        aria-label={label}
        // Scrolls on its own, so it takes focus for the keyboard to scroll it.
        tabIndex={0}
        className={`rounded-md border border-border-subtle bg-bg px-3 py-2 text-sm ${
          expanded ? '' : 'max-h-96 overflow-y-auto'
        }`}
      >
        <MarkdownContent content={text} />
      </div>
      <div className="mt-1.5 flex items-center gap-3">
        <Button
          variant="link"
          size="xs"
          aria-expanded={expanded}
          aria-controls={regionId}
          onClick={() => setExpanded(e => !e)}
        >
          {expanded ? <ChevronUp size={12} aria-hidden="true" /> : <ChevronDown size={12} aria-hidden="true" />}
          {expanded ? 'Show less' : 'Show all'}
        </Button>
        {onEdit && (
          <Button ref={editRef} variant="link" size="xs" onClick={onEdit}>
            <Pencil size={12} aria-hidden="true" />
            {PLAN_EDIT}
          </Button>
        )}
      </div>
    </div>
  );
}
