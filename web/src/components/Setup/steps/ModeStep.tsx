import { useId, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  api, type SetupMode, type SetupModeDetail, type SetupStep, type WorkCatalogItem, type WorkSelection,
} from '../../../api/client';
import { onboardingCanStart, useSetupStore } from '../../../stores/setupStore';
import { Badge, Checkbox, TextArea, cx } from '../../ui';
import { ChoiceCards } from '../ChoiceCards';
import {
  MODE_CARDS, ONBOARDING_OPEN, ONBOARDING_OPEN_SESSION, STEP_HEADINGS, WORK_CUSTOM_FIELD, WORK_CUSTOM_LABEL,
  WORK_CUSTOM_MISSING, WORK_CUSTOM_PLACEHOLDER, WORK_GROUPS, WORK_HEADING, WORK_INTRO, WORK_RULES_LABEL,
  WORK_RULES_PLACEHOLDER, WORK_TOO_LONG, onboardingSentence, workGroupHeading,
} from '../copy';
import { Field } from '../Field';
import { Notice } from '../Notice';
import { codePoints, same } from '../payload';
import { StepFrame } from '../StepFrame';

/** The server's caps on `work.custom` and `work.rules`, counted as it counts: in code points. */
const CUSTOM_MAX = 4000;
const RULES_MAX = 2000;

/** A worker's answers as the form holds them; `customOn` is the Custom… checkbox. */
interface WorkDraft {
  items: string[];
  customOn: boolean;
  custom: string;
  rules: string;
}

const draftOf = (work: WorkSelection | undefined): WorkDraft => ({
  items: work?.items ?? [],
  customOn: Boolean(work?.custom),
  custom: work?.custom ?? '',
  rules: work?.rules ?? '',
});

/** Ticking order is not an edit; the set of items is. */
const sameDraft = (a: WorkDraft, b: WorkDraft) => same(
  { ...a, items: [...a.items].sort() }, { ...b, items: [...b.items].sort() },
);

/**
 * The catalog by group: the known groups in their order (code · operations ·
 * communication · knowledge), then any group this build does not know, as
 * the server lists it. Within a group, items keep the server's order.
 */
function groupsOf(catalog: WorkCatalogItem[]): { group: string; items: WorkCatalogItem[] }[] {
  const groups = new Map<string, WorkCatalogItem[]>(WORK_GROUPS.map((g) => [g.id, []]));
  for (const item of catalog) groups.set(item.group, [...(groups.get(item.group) ?? []), item]);
  return [...groups].filter(([, items]) => items.length > 0).map(([group, items]) => ({ group, items }));
}

/** What `PUT /api/setup/mode` stores: ids in catalog order, Custom… only while ticked, text trimmed. */
function workPayload(draft: WorkDraft, catalog: WorkCatalogItem[]): WorkSelection {
  const ids = catalog.map((item) => item.id);
  const rank = (id: string) => (ids.includes(id) ? ids.indexOf(id) : ids.length);
  return {
    items: [...draft.items].sort((a, b) => rank(a) - rank(b)),
    custom: draft.customOn ? draft.custom.trim() : '',
    rules: draft.rules.trim(),
  };
}

/** Where a refusal from the server lands: beside its field while that field is on screen. */
type ErrorPlace = 'items' | 'custom' | 'rules' | 'other';

/**
 * One checkbox row. The first line is the checkbox's name; the description
 * and the note under it (a connection the work needs) are its description.
 * The whole row is the hit area.
 */
function WorkOption({ id, label, description, note, checked, onChange, disabled }: {
  id: string;
  label: string;
  description?: string;
  note?: string;
  checked: boolean;
  onChange: () => void;
  disabled?: boolean;
}) {
  const describedBy = [description && `${id}-description`, note && `${id}-note`].filter(Boolean).join(' ');
  return (
    <Checkbox
      checked={checked}
      onChange={onChange}
      disabled={disabled}
      aria-labelledby={`${id}-label`}
      aria-describedby={describedBy || undefined}
      className="mt-0.5 shrink-0"
      labelClassName={cx(
        'flex w-full items-start gap-2.5 rounded-lg border px-3 py-2 transition-colors',
        checked ? 'border-accent/60 bg-accent/5' : 'border-border-subtle',
        !disabled && !checked && 'hover:bg-surface-hover',
      )}
      label={(
        <span className="min-w-0">
          <span id={`${id}-label`} className="block text-sm text-text">{label}</span>
          {description && (
            <span id={`${id}-description`} className="block text-xs text-text-dim mt-0.5">{description}</span>
          )}
          {note && <span id={`${id}-note`} className="block text-xs text-text-muted italic mt-0.5">{note}</span>}
        </span>
      )}
    />
  );
}

/**
 * Personal assistant or Worker. Chosen once: the templates it writes are the
 * workspace's root files. Afterwards the step is read-only with the reason;
 * a worker whose templates arrived without a `TASK.md` may still write that.
 *
 * A worker gets the server's work catalog as checkboxes, grouped; Custom…
 * for other work in the person's own words; and an optional line for what
 * it must know or never do. All of it goes into `TASK.md`, and nothing
 * ticked is valid: the onboarding conversation asks the follow-up questions
 * (which repositories, which account), or offers the catalog when nothing
 * was chosen. The catalog is the server's; none of it is copied here.
 *
 * Personal assistant is preselected (and tagged Recommended) while the
 * choice is still open, so Continue is live on arrival; the preselection
 * is the form's opening value, not an edit, so Back never asks about it.
 */
export function ModeStep({ step }: { step: SetupStep }) {
  const detail = step.detail as SetupModeDetail;
  const save = useSetupStore((s) => s.save);
  const fieldError = useSetupStore((s) => s.fieldError);
  const canStart = useSetupStore((s) => onboardingCanStart(s.state));
  const [initialMode] = useState<SetupMode | null>(
    () => detail.mode ?? (step.status === 'pending' ? 'personal' : null),
  );
  const [mode, setMode] = useState<SetupMode | null>(initialMode);
  const [initialWork] = useState<WorkDraft>(() => draftOf(detail.work));
  const [work, setWork] = useState<WorkDraft>(initialWork);
  // Set when a tick on Custom… opens its field, which takes the focus once
  // as it appears — not when it reappears with the Worker card.
  const focusCustom = useRef(false);
  const [clientError, setClientError] = useState<string | null>(null);
  const [workErrors, setWorkErrors] = useState<{ custom?: string; rules?: string }>({});
  const uid = useId();

  const chosen = detail.mode !== null;
  const taskOnly = chosen && detail.mode === 'worker' && !detail.task_present && step.status !== 'blocked';
  const readOnly = (step.read_only || chosen) && !taskOnly;
  const catalog = detail.work_catalog ?? [];
  const showWork = mode === 'worker' && (!chosen || taskOnly);

  const field = fieldError?.field;
  const place: ErrorPlace | null = !field ? null
    : showWork && (field === 'work.items' || field === 'work') ? 'items'
      // `task_description` and `note` are what `work.custom` replaced.
      : showWork && work.customOn && ['work.custom', 'task_description', 'note'].includes(field) ? 'custom'
        : showWork && field === 'work.rules' ? 'rules'
          : 'other';
  const serverAt = (p: ErrorPlace) => (place === p ? fieldError?.detail ?? null : null);
  const itemsError = serverAt('items');
  const customError = workErrors.custom ?? serverAt('custom');
  const rulesError = workErrors.rules ?? serverAt('rules');
  const otherError = clientError ?? serverAt('other');

  const toggleItem = (id: string) => setWork((w) => ({
    ...w, items: w.items.includes(id) ? w.items.filter((x) => x !== id) : [...w.items, id],
  }));
  // Unticked, the field goes and its text with it; ticked again, it opens empty.
  const toggleCustom = () => {
    focusCustom.current = !work.customOn;
    setWorkErrors(({ rules }) => ({ rules }));
    setWork((w) => ({ ...w, customOn: !w.customOn, custom: '' }));
  };

  const submit = async () => {
    setClientError(null);
    setWorkErrors({});
    if (!mode) { setClientError('Choose one to continue.'); return false; }
    const body: Record<string, unknown> = { mode };
    if (mode === 'worker') {
      const payload = workPayload(work, catalog);
      const errors: { custom?: string; rules?: string } = {};
      if (work.customOn && !payload.custom) errors.custom = WORK_CUSTOM_MISSING;
      else if (codePoints(payload.custom) > CUSTOM_MAX) errors.custom = WORK_TOO_LONG(CUSTOM_MAX);
      if (codePoints(payload.rules) > RULES_MAX) errors.rules = WORK_TOO_LONG(RULES_MAX);
      if (errors.custom || errors.rules) { setWorkErrors(errors); return false; }
      body.work = payload;
    }
    return save('mode', () => api.setupStep('mode', body));
  };

  const onboarding = detail.onboarding;
  const sessionId = detail.onboarding_session_id;
  const kind = detail.onboarding_kind ?? 'conversation';
  const customErrorId = `${uid}-custom-error`;

  return (
    <StepFrame
      step={step}
      heading={STEP_HEADINGS.mode.heading}
      intro={STEP_HEADINGS.mode.intro}
      onSubmit={submit}
      readOnly={readOnly}
      dirty={mode !== initialMode || (mode === 'worker' && !sameDraft(work, initialWork))}
      canContinue={mode !== null}
    >
      <ChoiceCards
        label="Mode"
        options={MODE_CARDS.map((c) => ({
          value: c.value,
          title: c.recommended && !chosen
            ? <>{c.title} <Badge tone="accent" className="ml-1 align-middle">Recommended</Badge></>
            : c.title,
          description: c.description,
        }))}
        value={mode}
        onChange={setMode}
        disabled={readOnly || chosen}
        columns={2}
      />

      {chosen && !readOnly && taskOnly && (
        <Notice tone="info">The workspace templates are in place. Continue to create this worker's TASK.md.</Notice>
      )}

      {showWork && (
        <section aria-labelledby={`${uid}-work`} className="flex flex-col gap-3">
          <div>
            <h2 id={`${uid}-work`} className="text-sm font-medium text-text">{WORK_HEADING}</h2>
            <p className="text-xs text-text-muted mt-0.5">{WORK_INTRO}</p>
          </div>
          {groupsOf(catalog).map(({ group, items }) => (
            <div key={group} role="group" aria-labelledby={`${uid}-group-${group}`} className="flex flex-col gap-1.5">
              <h3 id={`${uid}-group-${group}`} className="text-xs font-medium text-text-muted">{workGroupHeading(group)}</h3>
              <ul className="flex flex-col gap-1.5">
                {items.map((item) => (
                  <li key={item.id}>
                    <WorkOption
                      id={`${uid}-item-${item.id}`}
                      label={item.label}
                      description={item.description}
                      note={item.note?.trim()}
                      checked={work.items.includes(item.id)}
                      onChange={() => toggleItem(item.id)}
                      disabled={readOnly}
                    />
                  </li>
                ))}
              </ul>
            </div>
          ))}
          <div className="flex flex-col gap-1.5">
            <WorkOption
              id={`${uid}-custom`}
              label={WORK_CUSTOM_LABEL}
              checked={work.customOn}
              onChange={toggleCustom}
              disabled={readOnly}
            />
            {work.customOn && (
              <>
                <TextArea
                  // Opened on purpose a moment ago, by Custom….
                  ref={(el) => {
                    if (el && focusCustom.current) {
                      focusCustom.current = false;
                      el.focus();
                    }
                  }}
                  rows={3}
                  value={work.custom}
                  onChange={(e) => setWork((w) => ({ ...w, custom: e.target.value }))}
                  aria-label={WORK_CUSTOM_FIELD}
                  placeholder={WORK_CUSTOM_PLACEHOLDER}
                  aria-invalid={customError ? true : undefined}
                  aria-describedby={customError ? customErrorId : undefined}
                  disabled={readOnly}
                />
                {customError && (
                  <p id={customErrorId} role="alert" tabIndex={-1} className="text-xs text-error outline-none">{customError}</p>
                )}
              </>
            )}
          </div>
          {itemsError && <p role="alert" tabIndex={-1} className="text-xs text-error outline-none">{itemsError}</p>}
        </section>
      )}

      {showWork && (
        <Field label={WORK_RULES_LABEL} error={rulesError}>
          <TextArea
            rows={2}
            value={work.rules}
            onChange={(e) => setWork((w) => ({ ...w, rules: e.target.value }))}
            placeholder={WORK_RULES_PLACEHOLDER}
            disabled={readOnly}
          />
        </Field>
      )}

      {/* The onboarding in sentences, never the enum or the session id: the
          link carries the id. */}
      {chosen && onboarding !== 'not_applicable' && (
        <p className="text-xs text-text-dim" role="status">
          {onboardingSentence(onboarding, kind, canStart)}
          {onboarding === 'running' && sessionId && (
            <>
              {' '}
              <Link to={`/chat/${sessionId}`} className="underline underline-offset-2 hover:text-text">
                {kind === 'autonomous' ? ONBOARDING_OPEN_SESSION : ONBOARDING_OPEN}
              </Link>
            </>
          )}
        </p>
      )}
      {otherError && <p role="alert" tabIndex={-1} className="text-sm text-error outline-none">{otherError}</p>}
    </StepFrame>
  );
}
