import { useState } from 'react';
import {
  api, type SetupAutomationDetail, type SetupCronJob, type SetupState, type SetupStep,
} from '../../../api/client';
import { useSetupStore } from '../../../stores/setupStore';
import { Badge, Checkbox } from '../../ui';
import { Clock } from '../../ui/icons';
import {
  AUTOMATION_IDLE_UNTIL_SOURCE, AUTOMATION_OVERRIDDEN, AUTOMATION_TASK_PLANNER_WORKER, STEP_HEADINGS,
} from '../copy';
import { StepFrame } from '../StepFrame';

/**
 * Whether the mode's first-run default leaves this job off. A backend that
 * sends neither field says nothing, and the job renders as it always did.
 */
const offByDefault = (job: SetupCronJob) => (job.default_enabled ?? job.default) === false;

/**
 * The cron catalogue for the chosen mode: core jobs listed as always-on,
 * optional ones as checkboxes with their schedule in the instance's time
 * zone. Always sends the full enabled set.
 *
 * The form opens on what the file enables, exactly — also on a pending step
 * (a terminal-configured or env-provisioned install opens Automation with
 * the first-run defaults on, and Continue must not silently switch one
 * off). A job whose requirement is not met (the inbox processor with no
 * source connected) stays checked and wears an "idle" badge that says why
 * it does nothing yet.
 *
 * A worker's task planner ships off: its onboarding conversation turns it
 * on when the worker should plan through the approval loop. While it is
 * still off, the row says so.
 *
 * Each checkbox shows the state the scheduler runs. A job that `jobs.yaml`
 * replaces (the onboarding conversation writes such jobs) shows that file's
 * state, cannot be changed here, and says where it is set.
 */
export function AutomationStep({ step, state }: { step: SetupStep; state: SetupState }) {
  const detail = step.detail as SetupAutomationDetail;
  const save = useSetupStore((s) => s.save);
  const fieldError = useSetupStore((s) => s.fieldError);
  const locked = step.read_only || step.status === 'blocked';
  const worker = state.mode === 'worker';
  const hintFor = (job: SetupCronJob) => (
    job.overridden ? AUTOMATION_OVERRIDDEN
      : worker && job.id === 'task-planner' && !job.enabled && offByDefault(job) ? AUTOMATION_TASK_PLANNER_WORKER
        : null
  );
  const [initialEnabled] = useState<string[]>(() => detail.optional.filter((j) => j.enabled).map((j) => j.id));
  const [enabled, setEnabled] = useState<string[]>(initialEnabled);

  const toggle = (id: string) => setEnabled((ids) => (
    ids.includes(id) ? ids.filter((x) => x !== id) : [...ids, id]
  ));

  const submit = () => save('automation', () => api.setupStep('automation', { crons: enabled }));
  const dirty = [...enabled].sort().join(',') !== [...initialEnabled].sort().join(',');
  const tz = detail.timezone || state.steps.find((s) => s.id === 'profile')?.summary || '';

  return (
    <StepFrame
      step={step}
      heading={STEP_HEADINGS.automation.heading}
      intro={STEP_HEADINGS.automation.intro}
      onSubmit={submit}
      readOnly={locked}
      dirty={dirty}
    >
      {!detail.file_present && (
        <p className="text-sm text-text-muted" role="status">Nerve creates the job list when you choose a mode.</p>
      )}

      <section aria-label="Core jobs" className="flex flex-col gap-2">
        <h2 className="text-sm font-medium text-text">Always on</h2>
        <ul className="flex flex-col gap-1">
          {detail.core.map((job) => (
            <li key={job.id} className="flex items-start justify-between gap-3 rounded-lg border border-border-subtle px-3 py-2">
              <div className="min-w-0">
                <p className="text-sm text-text">{job.name}</p>
                <p className="text-xs text-text-dim">{job.description}</p>
              </div>
              <span className="text-xs text-text-muted shrink-0 flex items-center gap-1">
                <Clock size={12} />{job.schedule_text}{tz ? ` (${tz})` : ''}
              </span>
            </li>
          ))}
          {detail.core.length === 0 && <li className="text-xs text-text-dim">None.</li>}
        </ul>
      </section>

      <section aria-label="Optional jobs" className="flex flex-col gap-2">
        <h2 className="text-sm font-medium text-text">Optional</h2>
        <ul className="flex flex-col gap-1">
          {detail.optional.map((job) => {
            const hint = hintFor(job);
            const hintId = `setup-cron-hint-${job.id}`;
            return (
              <li key={job.id} className="rounded-lg border border-border-subtle px-3 py-2">
                <div className="flex items-start justify-between gap-3">
                  <Checkbox
                    checked={enabled.includes(job.id)}
                    onChange={() => toggle(job.id)}
                    disabled={locked || job.overridden}
                    label={<span className="text-text">{job.name}</span>}
                    labelClassName="items-start"
                    aria-describedby={hint ? hintId : undefined}
                  />
                  <span className="text-xs text-text-muted shrink-0 flex items-center gap-1">
                    <Clock size={12} />{job.schedule_text}{tz ? ` (${tz})` : ''}
                  </span>
                </div>
                <p className="text-xs text-text-dim pl-6">{job.description}</p>
                {hint && <p id={hintId} className="text-xs text-text-muted pl-6 mt-0.5">{hint}</p>}
                {job.requires && (
                  <p className="text-xs pl-6 mt-0.5 flex items-center gap-1.5">
                    <Badge tone={job.requirement_met === false ? 'warning' : 'neutral'} size="xs">
                      {job.requirement_met === false ? 'idle' : 'uses'}
                    </Badge>
                    <span className="text-text-dim">
                      {job.requires}
                      {job.requirement_met === false && (
                        <> — {/source/i.test(job.requires) ? AUTOMATION_IDLE_UNTIL_SOURCE : 'not available yet'}</>
                      )}
                    </span>
                  </p>
                )}
              </li>
            );
          })}
          {detail.optional.length === 0 && <li className="text-xs text-text-dim">This mode offers no optional jobs.</li>}
        </ul>
      </section>
      {fieldError?.field === 'crons' && <p role="alert" className="text-sm text-error">{fieldError.detail}</p>}
    </StepFrame>
  );
}
