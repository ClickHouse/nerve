import { useId, useRef, useState, type FormEvent, type KeyboardEvent, type ReactNode } from 'react';
import { BookOpen, Check, MessageSquare, Pencil, Send, X } from '../ui/icons';
import { Button, TextArea } from '../ui';
import {
  PLAN_APPLY_EDITED, PLAN_DECLINE, PLAN_DECLINE_HINT, PLAN_DECLINE_LABEL, PLAN_DECLINE_SEND, PLAN_EDIT,
  PLAN_EDIT_CANCEL, PLAN_FEEDBACK_LABEL, PLAN_FEEDBACK_SEND, PLAN_READ, usePlanDecisionCopy,
} from './planDecision';

/** The text fields that a button of the controls opens. */
type Field = 'changes' | 'decline';

/**
 * What a decision callback gives back: `true` (or a promise of it) once the
 * decision is made, which closes and clears its field.
 */
type DecisionResult = void | boolean | Promise<void | boolean>;

/**
 * The plan decision's controls: the one implementation behind the plan card
 * in the chat, the plan panel's footer (labels from `planDecision.ts`) and
 * the decision bar of the plan page (its own labels).
 *
 * - **Approve**.
 * - **Request changes…** opens a feedback field. It must hold some text
 *   before Send sends it. Cmd/Ctrl+Enter sends; Escape closes the field and
 *   keeps what was typed.
 * - **Decline…** (given `onDecline`) opens an optional reason field and
 *   **Decline plan**, which may be sent empty. Its keys are as above.
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
  layout, labels, disabled = false, onApprove, onRequestChanges, onDecline, onReadPlan, onEdit, plan,
  onApproveEdited, editRequest,
}: {
  /**
   * `card`: the buttons in a row, the feedback field below them.
   * `footer`: compact buttons on the right, any field above them.
   * `bar`: as `footer` with full-size buttons; on a phone, Approve takes
   * a row of its own under the other two.
   * Either way the buttons stay where they are when a field opens.
   */
  layout: 'card' | 'footer' | 'bar';
  /** The words of Approve and Request changes, when not the chat's. */
  labels?: { approve: string; requestChanges: string };
  /** Disables every action, for example while a decision is sent. */
  disabled?: boolean;
  onApprove: () => void;
  /** Receives the trimmed feedback. Omitted: no Request changes is offered. */
  onRequestChanges?: (feedback: string) => DecisionResult;
  /** Receives the trimmed reason, which can be empty. Omitted: no Decline is offered. */
  onDecline?: (reason: string) => DecisionResult;
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
  const words = labels ?? copy;
  const [field, setField] = useState<Field | null>(null);
  const [feedback, setFeedback] = useState('');
  const [reason, setReason] = useState('');
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState('');
  const formId = useId();
  const fieldId = useId();
  const hintId = useId();
  const editorId = useId();
  const toggleRef = useRef<HTMLButtonElement>(null);
  const declineRef = useRef<HTMLButtonElement>(null);
  const editRef = useRef<HTMLButtonElement>(null);
  const card = layout === 'card';
  const compact = layout === 'footer';
  const declining = field === 'decline';
  const text = declining ? reason : feedback;
  const setText = declining ? setReason : setFeedback;
  const canSend = !disabled && (declining || feedback.trim().length > 0);
  const canEdit = !card && !!plan && !!onApproveEdited;
  const canApplyEdit = canEdit && draft.trim().length > 0 && draft.trim() !== plan.trim();

  // An Edit asked for from the card opens the editor here, once per request.
  // Set during render, React's pattern for acting on a changed prop.
  const [takenRequest, setTakenRequest] = useState<string | null>(null);
  if (editRequest && editRequest !== takenRequest) {
    setTakenRequest(editRequest);
    if (canEdit) {
      setDraft(plan);
      setField(null);
      setEditing(true);
    }
  }

  const send = async () => {
    if (!canSend) return;
    const decide = declining ? onDecline : onRequestChanges;
    if (await decide?.(text.trim()) === true) {
      setText('');
      setField(null);
    }
  };

  const onSubmit = (e: FormEvent) => {
    e.preventDefault();
    void send();
  };

  const onFieldKey = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
      e.preventDefault();
      void send();
    } else if (e.key === 'Escape') {
      e.preventDefault();
      e.stopPropagation();
      setField(null);
      (declining ? declineRef : toggleRef).current?.focus();
    }
  };

  const toggle = (which: Field) => setField(open => (open === which ? null : which));

  const startEdit = () => {
    if (!plan) return;
    setDraft(plan);
    setField(null);
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

  const size = compact ? 'sm' : 'md';
  const iconSize = compact ? 12 : 13;
  const secondaryClass = compact ? 'py-1 rounded-md font-medium' : '';

  const approve = (
    <Button
      variant={compact ? 'success' : 'primary'}
      size={size}
      className={card ? 'flex-1' : compact ? 'py-1' : 'col-span-2'}
      disabled={disabled}
      onClick={onApprove}
    >
      <Check size={iconSize} />
      {words.approve}
    </Button>
  );

  const fieldToggle = (which: Field, label: string, icon: ReactNode) => (
    <Button
      ref={which === 'decline' ? declineRef : toggleRef}
      variant="secondary"
      size={size}
      className={card ? 'flex-1' : secondaryClass}
      disabled={disabled}
      aria-expanded={field === which}
      aria-controls={field === which ? formId : undefined}
      onClick={() => toggle(which)}
    >
      {icon}
      {label}
    </Button>
  );
  const requestChanges = onRequestChanges
    && fieldToggle('changes', words.requestChanges, <MessageSquare size={iconSize} />);
  const decline = onDecline && fieldToggle('decline', PLAN_DECLINE, <X size={iconSize} />);

  const fieldForm = !editing && field && (declining ? onDecline : onRequestChanges) && (
    // A new form for each field, so the field that opens takes the focus.
    <form key={field} id={formId} onSubmit={onSubmit} className="space-y-1.5">
      <label htmlFor={fieldId} className="block text-xs font-medium text-text-secondary">
        {declining ? PLAN_DECLINE_LABEL : PLAN_FEEDBACK_LABEL}
      </label>
      <TextArea
        id={fieldId}
        value={text}
        onChange={e => setText(e.target.value)}
        onKeyDown={onFieldKey}
        required={!declining}
        aria-describedby={declining ? hintId : undefined}
        rows={declining ? 2 : 3}
        resizable
        // Opened on purpose a moment ago, by the button above or beside it.
        autoFocus
        className="max-h-60"
      />
      <div className="flex items-center justify-end gap-3">
        {declining && <p id={hintId} className="mr-auto text-xs text-text-muted">{PLAN_DECLINE_HINT}</p>}
        <Button type="submit" variant={declining ? 'dangerSolid' : 'primary'} size="sm" disabled={!canSend}>
          {declining ? <X size={12} /> : <Send size={12} />}
          {declining ? PLAN_DECLINE_SEND : PLAN_FEEDBACK_SEND}
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
        {fieldForm}
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
      {fieldForm}
      <div
        className={layout === 'bar'
          ? 'grid grid-cols-2 gap-2 md:flex md:items-center md:justify-end'
          : 'flex items-center justify-end gap-2'}
      >
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
            {decline}
            {requestChanges}
            {approve}
          </>
        )}
      </div>
    </div>
  );
}
