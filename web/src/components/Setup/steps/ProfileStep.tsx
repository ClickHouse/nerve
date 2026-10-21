import { useState } from 'react';
import { api, type SetupProfileDetail, type SetupState, type SetupStep } from '../../../api/client';
import { actorName } from '../../../stores/actorStore';
import { useAuthStore } from '../../../stores/authStore';
import { useSetupStore } from '../../../stores/setupStore';
import { useThemeStore } from '../../../stores/themeStore';
import { browserLanguageName, browserTimeZone, supportedTimeZones } from '../../../utils/timeZones';
import { Button, Checkbox, TextArea, TextField } from '../../ui';
import {
  COMMON_LANGUAGES, PROFILE_HEADING_WORKER, PROFILE_NAME_HELP_EXTERNAL, QUIET_HOURS_HELP, QUIET_HOURS_LABEL,
  QUIET_HOURS_PREFILL, STEP_HEADINGS,
} from '../copy';
import { Field } from '../Field';
import { Notice } from '../Notice';
import { codePoints, decides, quietEnabled, same } from '../payload';
import { focusRadio, radioTabIndex, radioTarget } from '../rovingRadio';
import { StepFrame } from '../StepFrame';

const TIME_RE = /^([01]\d|2[0-3]):[0-5]\d$/;

type ThemePref = 'system' | 'light' | 'dark';
const THEME_PREFS: readonly (readonly [ThemePref, string])[] = [['system', 'System'], ['light', 'Light'], ['dark', 'Dark']];
const THEME_VALUES: readonly ThemePref[] = THEME_PREFS.map(([pref]) => pref);
const never = () => false;

interface Form {
  name: string;
  timezone: string;
  /** Off hides the times and sends `quiet_enabled: false`; the times wait behind it. */
  quiet_enabled: boolean;
  quiet_start: string;
  quiet_end: string;
  language: string;
  agent_name: string;
  agent_emoji: string;
  about: string;
}

/**
 * About you (worker: Operator). Opens on what the server has, with three
 * client prefills — the browser's time zone when the server's is still the
 * default, 22:00–07:00 quiet hours while the step is pending and the file
 * still has the mode's default, and the browser language — and always sends
 * the time zone and the quiet-hours decision on a pending step, so accepting
 * a prefill is saved and recorded as answered.
 *
 * Quiet hours are optional: a switch above the two times, off by default
 * for a worker. They travel as a whole — `quiet_enabled: false` alone, or
 * `true` with both times — and a done step sends them only when the switch
 * or a time differs from the server's. While they are off the times hold
 * 22:00–07:00, so turning them on never shows two empty fields.
 *
 * In external mode the name comes from the actor header and only ever goes
 * to USER.md; no account controls render and no account endpoint is called.
 */
export function ProfileStep({ step, state }: { step: SetupStep; state: SetupState }) {
  const detail = step.detail as SetupProfileDetail;
  const save = useSetupStore((s) => s.save);
  const fieldError = useSetupStore((s) => s.fieldError);
  const themePref = useThemeStore((s) => s.preference);
  const setTheme = useThemeStore((s) => s.setTheme);
  const viewer = useAuthStore((s) => s.viewer);

  const external = state.auth_mode === 'external';
  const worker = state.mode === 'worker';
  const pending = decides(step);
  const onlyName = Array.isArray(detail.writable_fields)
    && detail.writable_fields.length === 1 && detail.writable_fields[0] === 'name';
  const locked = step.status === 'blocked' || (step.read_only && !onlyName);

  // The opening values: the server's, plus the client prefills. Kept as the
  // baseline for "dirty" so a prefill alone never triggers "Leave without
  // saving?" — the *payload* baseline is still the server's detail (§4.3).
  const [initial] = useState<Form>(() => {
    const zone = browserTimeZone();
    // Local mode, nothing on the server yet: the person just typed a
    // username on the claim, so start from that rather than a blank. In
    // external mode the header's name is already `detail.name` when there
    // is one, and a short-id label must never land in USER.md.
    const fallbackName = !external && pending && viewer ? actorName(viewer) : '';
    const prefill = detail.quiet_is_default && pending;
    const quietTime = (value: string, fallback: string) => ((prefill || !value) ? fallback : value);
    return {
      name: detail.name ?? fallbackName,
      timezone: detail.timezone_is_default && zone ? zone : detail.timezone,
      quiet_enabled: quietEnabled(detail),
      quiet_start: quietTime(detail.quiet_start, QUIET_HOURS_PREFILL.start),
      quiet_end: quietTime(detail.quiet_end, QUIET_HOURS_PREFILL.end),
      language: detail.language ?? (pending ? browserLanguageName() : ''),
      agent_name: detail.agent_name ?? '',
      agent_emoji: detail.agent_emoji ?? '',
      about: '',
    };
  });
  const [form, setForm] = useState<Form>(initial);
  const [clientError, setClientError] = useState<{ field: string; detail: string } | null>(null);

  const set = <K extends keyof Form>(key: K, value: Form[K]) => {
    setForm((f) => ({ ...f, [key]: value }));
    setClientError(null);
  };
  const errFor = (field: string) =>
    clientError?.field === field ? clientError.detail
      : fieldError?.field === field ? fieldError.detail : null;

  const submit = async () => {
    const name = form.name.trim();
    if (onlyName) {
      return save('profile', () => api.setupStep('profile', { name }));
    }
    if (!form.timezone.trim()) {
      setClientError({ field: 'timezone', detail: 'Pick a time zone.' }); return false;
    }
    if (form.quiet_enabled && (!TIME_RE.test(form.quiet_start) || !TIME_RE.test(form.quiet_end))) {
      setClientError({ field: 'quiet_start', detail: 'Use 24-hour times such as 22:00.' }); return false;
    }
    if (codePoints(form.agent_emoji.trim()) > 8) {
      setClientError({ field: 'agent_emoji', detail: 'Enter one emoji.' }); return false;
    }
    const body: Record<string, unknown> = {};
    const optional = ['language', 'agent_name', 'agent_emoji'] as const;
    const quiet = form.quiet_enabled
      ? { quiet_enabled: true, quiet_start: form.quiet_start, quiet_end: form.quiet_end }
      : { quiet_enabled: false };
    const quietChanged = form.quiet_enabled !== quietEnabled(detail) || (form.quiet_enabled
      && (!same(form.quiet_start, detail.quiet_start) || !same(form.quiet_end, detail.quiet_end)));
    if (pending) {
      // The decision payload: the time zone and the quiet hours, always.
      body.timezone = form.timezone.trim();
      Object.assign(body, quiet);
      if (name && (name !== (detail.name ?? '') || detail.user_md_pristine?.name)) body.name = name;
      for (const key of optional) {
        const value = form[key].trim();
        if (value && value !== (detail[key] ?? '')) body[key] = value;
      }
    } else {
      if (!same(form.timezone.trim(), detail.timezone)) body.timezone = form.timezone.trim();
      if (quietChanged) Object.assign(body, quiet);
      if (name !== (detail.name ?? '')) body.name = name;
      for (const key of optional) {
        const value = form[key].trim();
        if (value !== (detail[key] ?? '')) body[key] = value;
      }
    }
    if (form.about.trim()) body.about = form.about.trim();
    if (Object.keys(body).length === 0) return true;
    return save('profile', () => api.setupStep('profile', body));
  };

  const heading = worker ? PROFILE_HEADING_WORKER : STEP_HEADINGS.profile;
  const zone = browserTimeZone();
  // Times behind a switch that is off are not an edit.
  const shown = (f: Form) => (f.quiet_enabled ? f : { ...f, quiet_start: '', quiet_end: '' });
  const dirty = !same(shown(form), shown(initial));
  const fieldOff = (field: keyof Form) => locked || (onlyName && field !== 'name');
  const quietError = errFor('quiet_enabled');

  // Behind a front gateway the field shows what the *instance* holds: the
  // person's own header name until USER.md is filled, the saved USER.md name
  // after that (possibly somebody else's). The help says which, and always
  // who the server thinks is signed in.
  const actorNameLine = (detail.actor_name?.trim() || (viewer ? actorName(viewer) : '')) || null;
  const externalHelp = detail.name_source === 'user_md'
    ? PROFILE_NAME_HELP_EXTERNAL.user_md
    : PROFILE_NAME_HELP_EXTERNAL.actor_header;
  const nameHelp = external
    ? `${externalHelp}${actorNameLine ? ` ${PROFILE_NAME_HELP_EXTERNAL.signedInAs(actorNameLine)}` : ''}`
    : worker ? 'The person who operates this worker. It goes into the worker\'s notes.' : 'What Nerve calls you. It is also your display name in this app.';

  return (
    <StepFrame
      step={step}
      heading={heading.heading}
      intro={heading.intro}
      onSubmit={submit}
      readOnly={locked}
      dirty={dirty}
    >
      {onlyName && (
        <Notice tone="locked">
          {step.read_only_reason ?? state.read_only_reason ?? 'Configuration is managed from the config repository.'}
          {' '}Only your display name can be changed here.
        </Notice>
      )}

      <Field
        label="Your name"
        help={nameHelp}
        error={errFor('name')}
      >
        <TextField
          value={form.name}
          onChange={(e) => set('name', e.target.value)}
          autoComplete="name"
          maxLength={80}
          disabled={fieldOff('name')}
        />
      </Field>

      <Field
        label="Time zone"
        help="Scheduled jobs and quiet hours use this time zone."
        error={errFor('timezone')}
      >
        <TextField
          value={form.timezone}
          onChange={(e) => set('timezone', e.target.value)}
          list="setup-time-zones"
          autoComplete="off"
          placeholder="Europe/Berlin"
          disabled={fieldOff('timezone')}
        />
      </Field>
      <datalist id="setup-time-zones">
        {supportedTimeZones().map((z) => <option key={z} value={z} />)}
      </datalist>
      {zone && zone !== form.timezone && !fieldOff('timezone') && (
        <div>
          <Button variant="link" size="sm" onClick={() => set('timezone', zone)} className="min-h-[44px] md:min-h-0">
            Use my browser's time zone ({zone})
          </Button>
        </div>
      )}

      <div className="flex flex-col gap-1">
        <Checkbox
          checked={form.quiet_enabled}
          onChange={(e) => set('quiet_enabled', e.target.checked)}
          disabled={fieldOff('quiet_enabled')}
          label={<span className="font-medium text-text">{QUIET_HOURS_LABEL}</span>}
          aria-describedby={quietError ? 'setup-quiet-help setup-quiet-error' : 'setup-quiet-help'}
          aria-invalid={quietError ? true : undefined}
        />
        <p id="setup-quiet-help" className="text-xs text-text-dim">{QUIET_HOURS_HELP}</p>
        {quietError && (
          <p id="setup-quiet-error" role="alert" tabIndex={-1} className="text-xs text-error outline-none">{quietError}</p>
        )}
      </div>
      {form.quiet_enabled && (
        <div className="grid gap-3 sm:grid-cols-2">
          <Field label="Quiet hours start" error={errFor('quiet_start')}>
            <TextField
              type="time"
              value={form.quiet_start}
              onChange={(e) => set('quiet_start', e.target.value)}
              disabled={fieldOff('quiet_start')}
            />
          </Field>
          <Field label="Quiet hours end" error={errFor('quiet_end')}>
            <TextField
              type="time"
              value={form.quiet_end}
              onChange={(e) => set('quiet_end', e.target.value)}
              disabled={fieldOff('quiet_end')}
            />
          </Field>
        </div>
      )}

      <Field label="Response language" help="The language Nerve answers in." error={errFor('language')}>
        <TextField
          value={form.language}
          onChange={(e) => set('language', e.target.value)}
          list="setup-languages"
          autoComplete="off"
          maxLength={40}
          disabled={fieldOff('language')}
        />
      </Field>
      <datalist id="setup-languages">
        {COMMON_LANGUAGES.map((l) => <option key={l} value={l} />)}
      </datalist>

      {!worker && (
        <>
          <div className="grid gap-3 sm:grid-cols-[1fr_8rem]">
            <Field label="Agent name" help="What your assistant calls itself (IDENTITY.md)." error={errFor('agent_name')}>
              <TextField
                value={form.agent_name}
                onChange={(e) => set('agent_name', e.target.value)}
                placeholder="Nerve"
                autoComplete="off"
                maxLength={40}
                disabled={fieldOff('agent_name')}
              />
            </Field>
            <Field label="Emoji" error={errFor('agent_emoji')}>
              <TextField
                value={form.agent_emoji}
                onChange={(e) => set('agent_emoji', e.target.value)}
                placeholder="⚡"
                autoComplete="off"
                disabled={fieldOff('agent_emoji')}
              />
            </Field>
          </div>
          <Field
            label="A sentence about you or your work (optional)"
            help="Saved to USER.md, so Nerve knows it from the first conversation."
            error={errFor('about')}
          >
            <TextArea
              rows={2}
              value={form.about}
              onChange={(e) => set('about', e.target.value)}
              maxLength={600}
              placeholder="I run a two-person design studio."
              disabled={fieldOff('about')}
            />
          </Field>
        </>
      )}

      <div className="flex flex-col gap-1">
        <span className="text-sm font-medium text-text" id="setup-theme-label">Theme</span>
        {/* One tab stop; arrow keys move the choice (the radiogroup pattern). */}
        <div className="flex flex-wrap gap-1.5" role="radiogroup" aria-labelledby="setup-theme-label">
          {THEME_PREFS.map(([pref, label]) => (
            <Button
              key={pref}
              variant="pill"
              size="sm"
              role="radio"
              aria-checked={themePref === pref}
              active={themePref === pref}
              tabIndex={radioTabIndex(THEME_VALUES, themePref, pref, never)}
              onClick={() => setTheme(pref)}
              onKeyDown={(e) => {
                const target = radioTarget(THEME_VALUES, pref, e.key, never);
                if (target === null) return;
                e.preventDefault();
                setTheme(target);
                focusRadio(e.currentTarget, THEME_VALUES.indexOf(target));
              }}
            >
              {label}
            </Button>
          ))}
        </div>
        <p className="text-xs text-text-dim">Applies to this browser only. It changes right away and is not saved on the server.</p>
      </div>
    </StepFrame>
  );
}
