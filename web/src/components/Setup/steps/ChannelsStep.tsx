import { useState } from 'react';
import { api, type SetupChannelsDetail, type SetupState, type SetupStep } from '../../../api/client';
import { useSetupStore, type SetupFieldError } from '../../../stores/setupStore';
import { Button, Checkbox, TextField } from '../../ui';
import { ChevronDown, ChevronRight, MessageCircle, Slack } from '../../ui/icons';
import {
  NO_CHANNELS_LABEL, NO_LOCAL_SECRETS_TEXT, PAIRING_AFTER_RESTART, SLACK_DISCLOSURE, SLACK_GATEWAY_LINE,
  SLACK_GATEWAY_SWITCH, SLACK_INTRO, STEP_HEADINGS, TELEGRAM_STEPS,
} from '../copy';
import { secretManaged } from '../credentialSource';
import { Field } from '../Field';
import { keepsLocalSecrets } from '../navigation';
import { Notice } from '../Notice';
import { PairingWidget } from '../PairingWidget';
import { decides, same, secretPayload } from '../payload';
import { SecretField } from '../SecretField';
import { StepFrame } from '../StepFrame';
import { TestButton, type TestOutcome } from '../TestButton';

const TELEGRAM_TOKEN_RE = /^\d{6,12}:[A-Za-z0-9_-]{30,}$/;

function splitList(text: string): string[] {
  return text.split(/[,\s]+/).map((s) => s.trim()).filter(Boolean);
}

/** Which halves of the form the last PUT carried; decides whom a `bot_token` error belongs to. */
interface Sent { telegram: boolean; slack: boolean }

/**
 * Telegram (bot token → getMe → pairing) and Slack. Sends only what
 * changed. With nothing to send on a pending step, the primary button *is*
 * the decision — "No channels for now" records the skip and moves on — so
 * a first-time user without a bot never leaves this step still pending.
 * A `400 field: bot_token` from the PUT lands under the token field.
 *
 * Slack is behind a disclosure unless it is already configured, hosted, or
 * the server has refused one of its fields.
 *
 * A token the operator provided (environment, configuration repository)
 * renders as managed, with no Replace, Remove or Test. Slack held by the
 * shared channel gateway shows the gateway's status line and only the
 * switch; there are no tokens to enter.
 *
 * On an instance that keeps no credentials on disk (`local_secrets: false`)
 * this step is reached only when something made it done without a token
 * on this machine — a Slack the channel gateway holds, whose switch stays
 * the one control. The sentence replaces the token fields; a Telegram card
 * appears only for a token already configured, read-only.
 */
export function ChannelsStep({ step, state }: { step: SetupStep; state: SetupState }) {
  const detail = step.detail as SetupChannelsDetail;
  const save = useSetupStore((s) => s.save);
  const skip = useSetupStore((s) => s.skip);
  const fieldError = useSetupStore((s) => s.fieldError);
  const load = useSetupStore((s) => s.load);
  const locked = step.read_only || step.status === 'blocked';
  const keyless = !keepsLocalSecrets(state);
  const tg = detail.telegram;
  const slack = detail.slack;
  const slackHosted = slack.mode === 'hosted';
  const tgManaged = secretManaged(tg.bot_token) !== null;
  const slackManaged = secretManaged(slack.bot_token) !== null && secretManaged(slack.app_token) !== null;
  // A bot token would land in config.local.yaml: nothing about Telegram is
  // editable without local secrets, whatever the step's own status says.
  const tgLocked = locked || keyless;

  const [botToken, setBotToken] = useState<string | null>('');
  const [botName, setBotName] = useState<string | null>(null);
  const [manualUserId, setManualUserId] = useState('');
  const [slackBot, setSlackBot] = useState<string | null>('');
  const [slackApp, setSlackApp] = useState<string | null>('');
  const [slackEnabled, setSlackEnabled] = useState(slack.enabled);
  const [allowUsers, setAllowUsers] = useState(slack.allow_users.join(', '));
  const [allowDm, setAllowDm] = useState(slack.allow_direct_messages);
  const [allowChannels, setAllowChannels] = useState(slack.allow_channels.join(', '));
  const [slackOpen, setSlackOpen] = useState(
    slackHosted || slack.bot_token.configured || slack.app_token.configured,
  );
  const [clientError, setClientError] = useState<{ field: string; detail: string } | null>(null);
  // Which halves the last PUT carried. State, not a ref: it decides where
  // an error renders.
  const [lastSent, setLastSent] = useState<Sent>({ telegram: false, slack: false });

  // The server names both channels' token fields `bot_token`. Which card a
  // refusal belongs to follows from what was sent; when both halves went,
  // the sentence itself says which service answered.
  const slackOwns = (err: SetupFieldError): boolean => {
    if (lastSent.slack && !lastSent.telegram) return true;
    if (lastSent.telegram && !lastSent.slack) return false;
    return /slack/i.test(err.detail);
  };
  const serverError = (field: string, owner: 'telegram' | 'slack'): string | null => {
    if (!fieldError) return null;
    const f = fieldError.field;
    if (f === `${owner}.${field}` || f === `${owner}_${field}`) return fieldError.detail;
    if (f === field && (owner === 'slack') === slackOwns(fieldError)) return fieldError.detail;
    return null;
  };
  const errFor = (field: string, owner: 'telegram' | 'slack' = 'telegram') =>
    clientError?.field === field ? clientError.detail : serverError(field, owner);
  const slackErr = (field: 'bot_token' | 'app_token') =>
    clientError?.field === `slack_${field}` ? clientError.detail : serverError(field, 'slack');
  const slackHasError = slackErr('bot_token') !== null || slackErr('app_token') !== null
    || errFor('allow_users', 'slack') !== null || errFor('allow_channels', 'slack') !== null;
  const slackShown = slackOpen || slackHasError;

  /** Everything the form would send right now; `null` when there is nothing. */
  const buildBody = (): Record<string, unknown> | null | false => {
    const telegram: Record<string, unknown> = {};
    const token = secretPayload(botToken);
    if (token === null) telegram.bot_token = null;
    else if (token !== undefined) {
      if (!TELEGRAM_TOKEN_RE.test(token)) {
        setClientError({ field: 'bot_token', detail: 'That does not look like a bot token from @BotFather.' });
        return false;
      }
      telegram.bot_token = token;
      telegram.enabled = true;
    }
    if (manualUserId.trim()) {
      const id = Number(manualUserId.trim());
      if (!Number.isInteger(id) || id <= 0) {
        setClientError({ field: 'allowed_users_add', detail: 'A Telegram user id is a positive number.' });
        return false;
      }
      telegram.allowed_users_add = [id];
    }

    const slackBody: Record<string, unknown> = {};
    if (!slackHosted) {
      const bot = secretPayload(slackBot);
      const app = secretPayload(slackApp);
      if (bot !== undefined) {
        if (bot && !bot.startsWith('xoxb-')) { setClientError({ field: 'slack_bot_token', detail: 'Slack bot tokens start with xoxb-.' }); return false; }
        slackBody.bot_token = bot;
      }
      if (app !== undefined) {
        if (app && !app.startsWith('xapp-')) { setClientError({ field: 'slack_app_token', detail: 'Slack app tokens start with xapp-.' }); return false; }
        slackBody.app_token = app;
      }
      const users = splitList(allowUsers);
      if (!same(users, slack.allow_users)) slackBody.allow_users = users;
      if (allowDm !== slack.allow_direct_messages) slackBody.allow_direct_messages = allowDm;
      const channels = splitList(allowChannels);
      if (!same(channels, slack.allow_channels)) slackBody.allow_channels = channels;
      const botAfter = bot ? true : bot === null ? false : slack.bot_token.configured;
      const appAfter = app ? true : app === null ? false : slack.app_token.configured;
      if ((bot || app) && botAfter && appAfter && !slack.enabled) slackBody.enabled = true;
    }
    if (slackEnabled !== slack.enabled) slackBody.enabled = slackEnabled;

    const body: Record<string, unknown> = {};
    if (Object.keys(telegram).length > 0) body.telegram = telegram;
    if (Object.keys(slackBody).length > 0) body.slack = slackBody;
    return Object.keys(body).length === 0 ? null : body;
  };

  const submit = async () => {
    setClientError(null);
    const body = buildBody();
    if (body === false) return false;
    if (body === null) {
      // Nothing entered. On a pending step that is the "no channels"
      // decision; on a skipped or done step there is nothing to do.
      return step.status === 'pending' ? skip('channels', true) : true;
    }
    setLastSent({ telegram: 'telegram' in body, slack: 'slack' in body });
    const ok = await save('channels', () => api.setupStep('channels', body));
    if (ok) { setBotToken(''); setManualUserId(''); setSlackBot(''); setSlackApp(''); }
    return ok;
  };

  const testTelegram = async (): Promise<TestOutcome> => {
    const result = await api.setupTelegramTest({ bot_token: (botToken ?? '').trim() });
    if (result.ok && result.bot_username) setBotName(result.bot_username);
    return {
      ok: result.ok,
      detail: result.ok ? `Token works: @${result.bot_username ?? 'bot'}` : result.detail,
      hint: result.hint,
    };
  };

  const testSlack = async (): Promise<TestOutcome> => {
    const result = await api.setupSlackTest({
      bot_token: (slackBot ?? '').trim(), app_token: (slackApp ?? '').trim(),
    });
    return {
      ok: result.ok,
      detail: result.ok ? `Connected to ${result.team ?? 'Slack'} as ${result.bot_user ?? 'the bot'}` : result.detail,
      hint: result.hint,
    };
  };

  const dirty = (botToken ?? '') !== '' || botToken === null || manualUserId !== ''
    || (slackBot ?? '') !== '' || slackBot === null || (slackApp ?? '') !== '' || slackApp === null
    || slackEnabled !== slack.enabled || allowUsers !== slack.allow_users.join(', ')
    || allowDm !== slack.allow_direct_messages || allowChannels !== slack.allow_channels.join(', ');
  // With nothing typed on a pending step the primary button performs the skip.
  const primaryIsSkip = !locked && decides(step) && step.status === 'pending' && !dirty && step.can_skip;

  return (
    <StepFrame
      step={step}
      heading={STEP_HEADINGS.channels.heading}
      intro={STEP_HEADINGS.channels.intro}
      onSubmit={submit}
      readOnly={locked}
      dirty={dirty}
      continueLabel={primaryIsSkip ? NO_CHANNELS_LABEL : undefined}
      hideSkip={primaryIsSkip}
    >
      {keyless && <Notice tone="locked">{NO_LOCAL_SECRETS_TEXT}</Notice>}

      {(!keyless || tg.bot_token.configured) && (
        <section aria-label="Telegram" className="flex flex-col gap-3 rounded-lg border border-border bg-surface-raised p-4">
          <h2 className="text-sm font-medium text-text flex items-center gap-2"><MessageCircle size={16} />Telegram</h2>
          {tg.start_error && (
            <Notice tone="error">
              The bot could not start with the saved token: {tg.start_error} Replace the token and restart.
            </Notice>
          )}
          {!tg.bot_token.configured && (
            <ol className="list-decimal pl-5 text-sm text-text-muted flex flex-col gap-0.5">
              {TELEGRAM_STEPS.map((line) => <li key={line}>{line}</li>)}
            </ol>
          )}
          {tg.bot_token.configured && (
            <p className="text-sm text-text-muted">
              {tg.bot_username ? <>Bot <strong className="text-text">@{tg.bot_username}</strong></> : 'A bot token is saved'}
              {tg.live ? ' · running' : ' · starts after the restart'}
              {tg.allowed_users_count > 0 ? ` · ${tg.allowed_users_count} paired` : ' · not paired yet'}
            </p>
          )}
          <SecretField
            label="Bot token"
            secret={tg.bot_token}
            value={botToken}
            onChange={(v) => { setBotToken(v); setBotName(null); setClientError(null); }}
            placeholder="123456789:AA…"
            error={errFor('bot_token')}
            disabled={tgLocked}
          />
          {!tgLocked && !tgManaged && (
            <TestButton label="Test token" run={testTelegram} disabled={!(botToken ?? '').trim()} />
          )}
          {botName && <p className="text-xs text-text-dim">Saving connects @{botName}.</p>}

          {tg.bot_token.configured && (
            tg.pairing_available
              ? <PairingWidget onPaired={() => void load()} />
              : (
                <div className="flex flex-col gap-2">
                  <Notice tone="info">{tg.pairing_unavailable_reason ?? PAIRING_AFTER_RESTART}</Notice>
                  <Field
                    label="Your numeric Telegram user id (optional)"
                    help="Pairs your account without a code. Send /start to @userinfobot in Telegram to see your id."
                    error={errFor('allowed_users_add')}
                  >
                    <TextField
                      value={manualUserId}
                      onChange={(e) => { setManualUserId(e.target.value); setClientError(null); }}
                      inputMode="numeric"
                      autoComplete="off"
                      placeholder="123456789"
                      disabled={tgLocked}
                    />
                  </Field>
                </div>
              )
          )}
        </section>
      )}

      {!slackShown ? (
        // No tokens to enter without local secrets: nothing to disclose.
        !keyless && (
          <div>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setSlackOpen(true)}
              aria-expanded={false}
              aria-controls="setup-slack-section"
              className="min-h-[44px]"
            >
              <ChevronRight size={14} /><Slack size={14} />{SLACK_DISCLOSURE}
            </Button>
          </div>
        )
      ) : (
        <section id="setup-slack-section" aria-label="Slack" className="flex flex-col gap-3 rounded-lg border border-border bg-surface-raised p-4">
          <h2 className="text-sm font-medium text-text flex items-center justify-between gap-2">
            <span className="flex items-center gap-2"><Slack size={16} />Slack</span>
            {!slackHosted && !slack.bot_token.configured && !slack.app_token.configured && !slackHasError && (
              <Button variant="ghost" size="xs" onClick={() => setSlackOpen(false)} aria-expanded aria-controls="setup-slack-section" className="min-h-[44px] md:min-h-0">
                <ChevronDown size={12} />Hide
              </Button>
            )}
          </h2>
          {slackHosted ? (
            <>
              {/* The gateway's stream count stays out of the copy: it is the
                  gateway's internal detail, not something a person acts on. */}
              <Notice tone="locked">
                {SLACK_GATEWAY_LINE(slack.gateway?.connected === true)}. {SLACK_GATEWAY_SWITCH}
              </Notice>
              <Checkbox
                checked={slackEnabled}
                onChange={(e) => setSlackEnabled(e.target.checked)}
                label="Slack enabled"
                disabled={locked}
              />
            </>
          ) : (
            <>
              <p className="text-sm text-text-muted">{SLACK_INTRO}</p>
              <SecretField
                label="Bot token"
                secret={slack.bot_token}
                value={slackBot}
                onChange={(v) => { setSlackBot(v); setClientError(null); }}
                placeholder="xoxb-…"
                error={slackErr('bot_token')}
                disabled={locked}
              />
              <SecretField
                label="App-level token"
                secret={slack.app_token}
                value={slackApp}
                onChange={(v) => { setSlackApp(v); setClientError(null); }}
                placeholder="xapp-…"
                error={slackErr('app_token')}
                disabled={locked}
              />
              <Field label="Allowed Slack user ids" help="Comma-separated member ids such as U0123456789. Empty means nobody can message the bot." error={errFor('allow_users', 'slack')}>
                <TextField value={allowUsers} onChange={(e) => setAllowUsers(e.target.value)} autoComplete="off" disabled={locked} />
              </Field>
              <Checkbox
                checked={allowDm}
                onChange={(e) => setAllowDm(e.target.checked)}
                label="Allow direct messages"
                disabled={locked}
              />
              <Field label="Allowed channels (optional)" help="Channel ids the bot may answer in." error={errFor('allow_channels', 'slack')}>
                <TextField value={allowChannels} onChange={(e) => setAllowChannels(e.target.value)} autoComplete="off" disabled={locked} />
              </Field>
              {(slack.bot_token.configured || slack.app_token.configured) && (
                <Checkbox
                  checked={slackEnabled}
                  onChange={(e) => setSlackEnabled(e.target.checked)}
                  label="Slack enabled"
                  disabled={locked}
                />
              )}
              {!locked && !slackManaged && !keyless && (
                <TestButton
                  label="Test Slack"
                  run={testSlack}
                  disabled={!(slackBot ?? '').trim() || !(slackApp ?? '').trim()}
                />
              )}
            </>
          )}
        </section>
      )}
      {state.lockdown && !locked && (
        <p className="text-xs text-text-dim">Channel tokens are stored on this server, not in the repository, so you can enter them here.</p>
      )}
    </StepFrame>
  );
}
