import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Bell, X, CheckCheck, EyeOff, Check, XCircle, Moon, BellOff, Trash2, Plus, RotateCw, Clock } from '../components/ui/icons';
import { Badge, Button, IconButton, TextField, type BadgeTone, type ButtonVariant } from '../components/ui';
import { useNotificationStore, type Notification, type Silence } from '../stores/notificationStore';
import { PageHeader } from '../components/ui/PageHeader';

/** Lifecycle state — a status, so it takes a `Badge` tone. */
const STATUS_TONES: Record<string, BadgeTone> = {
  pending: 'warning',
  answered: 'success',
  expired: 'neutral',
  dismissed: 'neutral',
  silenced: 'neutral',
};

/**
 * Priority dot. Only urgent and high get one; `bg-error`/`bg-warning` are the
 * solid-fill status tokens, so the dot tracks the theme.
 */
const PRIORITY_DOTS: Record<string, string> = {
  urgent: 'bg-error',
  high: 'bg-warning',
  normal: '',
  low: '',
};

/** Which kind of notification this is — identity, not status. */
const TYPE_BADGE_TONES: Record<string, BadgeTone> = {
  question: 'info',
  approval: 'purple',
  notify: 'neutral',
};

const STATUS_FILTERS = [
  { label: 'All', value: '' },
  { label: 'Pending', value: 'pending' },
  { label: 'Answered', value: 'answered' },
  { label: 'Expired', value: 'expired' },
  { label: 'Silenced', value: 'silenced' },
];

const TYPE_FILTERS = [
  { label: 'All', value: '' },
  { label: 'Notifications', value: 'notify' },
  { label: 'Questions', value: 'question' },
  { label: 'Approvals', value: 'approval' },
];

// Approval-kind button styling. Keyed by the option ``value`` the
// dispatcher receives, not the human label, so the styling stays
// stable even when labels are renamed.
const APPROVAL_BUTTON_VARIANTS: Record<string, ButtonVariant> = {
  approve: 'success',
  decline: 'danger',
  snooze_24h: 'secondary',
};

const APPROVAL_BUTTON_ICONS: Record<string, typeof Check> = {
  approve: Check,
  decline: XCircle,
  snooze_24h: Moon,
};

const APPROVAL_DEFAULT_LABELS: Record<string, string> = {
  approve: 'Approve',
  decline: 'Decline',
  snooze_24h: 'Snooze 24h',
};

function approvalLabel(value: string, labels: Record<string, string> | null | undefined): string {
  if (labels && labels[value]) return labels[value];
  if (APPROVAL_DEFAULT_LABELS[value]) return APPROVAL_DEFAULT_LABELS[value];
  // Fall back to value with underscores replaced and title cased.
  return value
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function parseOptionLabels(notif: Notification): Record<string, string> | null {
  if (notif.option_labels) return notif.option_labels;
  if (!notif.metadata) return null;
  try {
    const meta = typeof notif.metadata === 'string' ? JSON.parse(notif.metadata) : notif.metadata;
    if (meta && typeof meta === 'object' && 'option_labels' in meta) {
      const labels = (meta as Record<string, unknown>).option_labels;
      if (labels && typeof labels === 'object') {
        return labels as Record<string, string>;
      }
    }
  } catch {
    // Malformed JSON: just fall back to defaults.
  }
  return null;
}

interface SilenceInfo {
  silenced_by?: string;
  silence_reason?: string;
  silence_pattern?: string;
  silence_hit_count?: number;
}

function parseSilenceInfo(notif: Notification): SilenceInfo | null {
  if (!notif.metadata) return null;
  try {
    const meta = typeof notif.metadata === 'string' ? JSON.parse(notif.metadata) : notif.metadata;
    if (meta && typeof meta === 'object' && 'silenced_by' in meta) {
      return meta as SilenceInfo;
    }
  } catch {
    // Malformed JSON: nothing to surface.
  }
  return null;
}

function formatExpiry(expiresAt: string | null): string {
  if (!expiresAt) return 'permanent';
  return `expires ${expiresAt.slice(0, 16).replace('T', ' ')}`;
}

function formatLocalTime(iso: string): string {
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso.slice(0, 16).replace('T', ' ');
  return d.toLocaleString(undefined, {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
  });
}

function FreeTextInput({ onSubmit }: { onSubmit: (text: string) => void }) {
  const [text, setText] = useState('');
  const [open, setOpen] = useState(false);

  if (!open) {
    return (
      <Button
        variant="ghost"
        size="md"
        onClick={() => setOpen(true)}
        className="border border-dashed border-border rounded-lg hover:border-border-subtle"
      >
        Custom answer...
      </Button>
    );
  }

  return (
    <div className="flex items-center gap-2 w-full mt-1">
      <TextField
        autoFocus
        value={text}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' && text.trim()) {
            onSubmit(text.trim());
            setText('');
            setOpen(false);
          }
          if (e.key === 'Escape') setOpen(false);
        }}
        fullWidth={false}
        className="flex-1"
        placeholder="Type your answer..."
      />
      <Button
        variant="accentSoft"
        size="md"
        onClick={() => {
          if (text.trim()) {
            onSubmit(text.trim());
            setText('');
            setOpen(false);
          }
        }}
      >
        Send
      </Button>
      <IconButton size="xs" label="Cancel" onClick={() => { setText(''); setOpen(false); }}>
        <X size={14} />
      </IconButton>
    </div>
  );
}

function NotificationCard({ notif }: { notif: Notification }) {
  const navigate = useNavigate();
  const { answerNotification, dismissNotification } = useNotificationStore();
  const priorityDot = PRIORITY_DOTS[notif.priority];
  const options = notif.options ? (typeof notif.options === 'string' ? JSON.parse(notif.options) : notif.options) : null;
  const isApproval = notif.type === 'approval';
  const optionLabels = isApproval ? parseOptionLabels(notif) : null;

  return (
    <div className={`p-4 bg-surface border rounded-lg transition-colors ${
      notif.status === 'pending' ? 'border-border-subtle' : 'border-border-subtle'
    }`}>
      {/* The badge column is shrink-0, so beside the text it costs a fixed
          ~127px — over a third of the card on a phone, which left the body
          wrapping at ~190px. Below `sm` the badges take a row of their own
          above the title instead, and the text gets the full card width. */}
      <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-2 sm:gap-3">
        <div className="min-w-0 flex-1 order-1 sm:order-none">
          {/* items-start, not items-center: a title long enough to wrap was
              centring the priority dot against the whole block, leaving it
              floating beside the second line. */}
          <div className="flex items-start gap-2">
            {priorityDot && <span className={`w-2 h-2 mt-1.5 rounded-full shrink-0 ${priorityDot}`} />}
            <h3 className="font-medium text-base text-text">{notif.title}</h3>
          </div>
          {notif.body && (
            <p className="text-sm text-text-muted mt-1 whitespace-pre-wrap">{notif.body}</p>
          )}
          {isApproval && notif.target_kind && notif.target_id && (
            <p className="text-xs text-text-faint mt-1 font-mono">
              {notif.target_kind}: {notif.target_id}
            </p>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-2 shrink-0 order-0 sm:order-none">
          {(notif.redelivery_count ?? 0) > 0 && (
            <Badge
              tone="info"
              size="sm"
              pill
              outline
              title={`Re-delivered after snooze (cycle ${notif.redelivery_count})`}
            >
              <RotateCw size={10} />
              {(notif.redelivery_count ?? 0) > 1 ? `×${notif.redelivery_count}` : 're-delivered'}
            </Badge>
          )}
          <Badge tone={STATUS_TONES[notif.status] || 'neutral'} size="sm" pill outline>
            {notif.status}
          </Badge>
          <Badge tone={TYPE_BADGE_TONES[notif.type] || 'neutral'} size="sm" pill outline>
            {notif.type}
          </Badge>
        </div>
      </div>

      {/* Session link + meta */}
      <div className="flex items-center gap-3 mt-2 text-xs">
        <Button
          variant="link"
          size="xs"
          onClick={() => navigate(`/chat/${notif.session_id}`)}
          className="min-w-0 shrink whitespace-normal wrap-anywhere text-left"
        >
          Session: {notif.session_title || notif.session_id}
        </Button>
        <span className="text-text-faint">{notif.created_at?.slice(0, 16).replace('T', ' ')}</span>
        {notif.status === 'pending' && notif.type === 'notify' && (
          <Button variant="ghost" size="xs" onClick={() => dismissNotification(notif.id)}>
            <EyeOff size={11} />
            <span>Dismiss</span>
          </Button>
        )}
      </div>

      {/* Answer UI for pending questions */}
      {notif.type === 'question' && notif.status === 'pending' && (
        <div className="mt-3 flex flex-wrap gap-2">
          {options?.map((opt: string) => (
            <Button
              key={opt}
              variant="accentSoft"
              size="md"
              onClick={() => answerNotification(notif.id, opt)}
            >
              {opt}
            </Button>
          ))}
          <FreeTextInput onSubmit={(text) => answerNotification(notif.id, text)} />
        </div>
      )}

      {/* Action UI for pending approvals */}
      {isApproval && notif.status === 'pending' && (
        <div className="mt-3 flex flex-wrap gap-2">
          {options?.map((value: string) => {
            const Icon = APPROVAL_BUTTON_ICONS[value];
            // `accentSoft` for anything outside approve/decline/snooze_24h, so
            // an unrecognised option still reads as the same kind of control as
            // the `success`/`danger` siblings beside it.
            const variant = APPROVAL_BUTTON_VARIANTS[value] || 'accentSoft';
            const label = approvalLabel(value, optionLabels);
            return (
              <Button
                key={value}
                variant={variant}
                size="md"
                className="gap-1.5"
                onClick={() => answerNotification(notif.id, value)}
              >
                {Icon ? <Icon size={14} /> : null}
                <span>{label}</span>
              </Button>
            );
          })}
        </div>
      )}

      {/* Snoozed: still pending server-side, will be re-delivered */}
      {notif.status === 'pending' && notif.redeliver_at && (
        <div className="mt-2 flex items-center gap-1.5 text-xs text-text-dim">
          <Clock size={12} className="shrink-0" />
          <span>
            Snoozed — returns {formatLocalTime(notif.redeliver_at)}. You can still decide now.
          </span>
        </div>
      )}

      {/* Show answer if answered */}
      {notif.status === 'answered' && (
        <div className="mt-2 text-sm text-success">
          Answer: {isApproval ? approvalLabel(notif.answer || '', optionLabels) : notif.answer}{' '}
          <span className="text-text-faint">(via {notif.answered_by})</span>
        </div>
      )}

      {/* Silence context: why this was suppressed and not delivered */}
      {notif.status === 'silenced' && (() => {
        const info = parseSilenceInfo(notif);
        return (
          <div className="mt-2 flex items-start gap-1.5 text-xs text-text-dim">
            <BellOff size={12} className="mt-0.5 shrink-0" />
            <div className="min-w-0">
              <span>Silenced{info?.silenced_by ? ` by ${info.silenced_by}` : ''} — not delivered.</span>
              {info?.silence_reason && (
                <span className="text-text-muted"> {info.silence_reason}</span>
              )}
              {info?.silence_pattern && (
                <span className="ml-1 font-mono text-text-faint">[{info.silence_pattern}]</span>
              )}
            </div>
          </div>
        );
      })()}
    </div>
  );
}

function SilenceRow({ s, onRemove }: { s: Silence; onRemove: () => void }) {
  const overridden = s.override_count > 0;
  return (
    <div className="flex items-center gap-3 px-3 py-2 bg-surface-raised border border-border-subtle rounded-lg text-xs">
      <code className="font-mono text-text-muted shrink-0">{s.id}</code>
      <code className="font-mono text-accent truncate flex-1 min-w-0">{s.pattern}</code>
      <span className="text-text-muted shrink-0">
        {s.hit_count} hits
        {overridden ? `, ${s.override_count} override${s.override_count !== 1 ? 's' : ''}` : ''}
      </span>
      <span className="text-text-faint shrink-0">{formatExpiry(s.expires_at)}</span>
      {s.reason && (
        <span className="text-text-dim truncate hidden md:block max-w-[14rem]" title={s.reason}>
          {s.reason}
        </span>
      )}
      {overridden && (
        <span className="text-warning shrink-0" title="Force-overridden — this pattern may be too broad">⚠</span>
      )}
      <IconButton size="xs" variant="dangerGhost" label="Remove silence" onClick={onRemove}>
        <Trash2 size={13} />
      </IconButton>
    </div>
  );
}

function SilencesPanel() {
  const { silences, loadSilences, addSilence, removeSilence } = useNotificationStore();
  const [pattern, setPattern] = useState('');
  const [reason, setReason] = useState('');
  const [ttlHours, setTtlHours] = useState('');
  const [error, setError] = useState('');
  const [adding, setAdding] = useState(false);

  useEffect(() => { loadSilences(); }, []);

  const submit = async () => {
    const p = pattern.trim();
    if (!p) { setError('Pattern is required'); return; }
    try { new RegExp(p); } catch { setError('Invalid regex'); return; }
    setAdding(true);
    setError('');
    try {
      await addSilence(p, reason.trim(), Number(ttlHours) || 0);
      setPattern(''); setReason(''); setTtlHours('');
    } catch (e: any) {
      setError(e?.message || 'Failed to create silence');
    } finally {
      setAdding(false);
    }
  };

  return (
    <div className="max-w-3xl mx-auto mb-4 p-4 bg-surface border border-border-subtle rounded-lg">
      <div className="flex items-center gap-2 mb-3 flex-wrap">
        <BellOff size={15} className="text-text-muted" />
        <h2 className="text-sm font-semibold text-text">Silences</h2>
        <span className="text-xs text-text-faint">
          Suppress known-benign alert classes — matched notifications are recorded but not delivered (priority unchanged).
        </span>
      </div>

      <div className="flex flex-wrap items-center gap-2 mb-2">
        <TextField
          value={pattern}
          onChange={(e) => setPattern(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter') submit(); }}
          placeholder="Regex pattern (matches title + body)"
          fullWidth={false}
          className="flex-1 min-w-[14rem] font-mono"
        />
        <TextField
          value={reason}
          onChange={(e) => setReason(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter') submit(); }}
          placeholder="Reason"
          fullWidth={false}
          className="flex-1 min-w-[10rem]"
        />
        <TextField
          type="number"
          min="0"
          value={ttlHours}
          onChange={(e) => setTtlHours(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter') submit(); }}
          placeholder="TTL h (0=∞)"
          fullWidth={false}
          className="w-28"
        />
        <Button variant="accentSoft" size="md"
          onClick={submit} disabled={adding}>
          <Plus size={14} /> Add
        </Button>
      </div>
      {error && <div className="text-xs text-error mb-2">{error}</div>}

      {silences.length === 0 ? (
        <div className="text-xs text-text-faint">No silences configured.</div>
      ) : (
        <div className="space-y-1.5">
          {silences.map((s) => (
            <SilenceRow key={s.id} s={s} onRemove={() => removeSilence(s.id)} />
          ))}
        </div>
      )}
    </div>
  );
}

export function NotificationsPage() {
  const {
    notifications, pendingCount, filter, typeFilter, loading,
    loadNotifications, setFilter, setTypeFilter, dismissAll,
  } = useNotificationStore();
  const [showSilences, setShowSilences] = useState(false);

  useEffect(() => { loadNotifications(); }, []);

  return (
    <div className="h-full flex flex-col">
      <PageHeader
        icon={<Bell size={18} className="text-accent shrink-0" />}
        title="Notifications"
        filters={
          <>
            {/* Status filters */}
            {STATUS_FILTERS.map(f => (
              <Button key={f.value} variant="pill" active={filter === f.value}
                onClick={() => setFilter(f.value)}>
                {f.label}
              </Button>
            ))}
            {/* Type filters — separated by a rule rather than the old ml-1,
                which read as one undifferentiated run of pills once they
                shared a scroller. */}
            <span className="mx-1 h-4 w-px shrink-0 bg-border-subtle" aria-hidden="true" />
            {TYPE_FILTERS.map(f => (
              <Button key={f.value} variant="pill" active={typeFilter === f.value}
                onClick={() => setTypeFilter(f.value)}>
                {f.label}
              </Button>
            ))}
          </>
        }
        actions={
          <>
          {/* A toggle, not a filter chip, so it keeps the rounded-lg button
              shape — `pill` supplies the same active treatment either way. */}
          <Button variant="pill" active={showSilences}
            className="rounded-lg whitespace-nowrap"
            onClick={() => setShowSilences(v => !v)}>
            <BellOff size={13} />
            Silences
          </Button>
          {/* Dismiss All */}
          {pendingCount > 0 && (
            <Button variant="secondary" className="whitespace-nowrap" onClick={dismissAll}>
              <CheckCheck size={13} />
              Dismiss All
            </Button>
          )}
          </>
        }
      />

      <div className="flex-1 overflow-y-auto p-4 md:p-6">
        {showSilences && <SilencesPanel />}
        {loading ? (
          <div className="text-text-faint text-center py-10">Loading...</div>
        ) : notifications.length === 0 ? (
          <div className="text-text-faint text-center py-10">
            {filter || typeFilter ? 'No matching notifications' : 'No notifications yet.'}
          </div>
        ) : (
          <div className="max-w-3xl mx-auto space-y-2">
            {notifications.map(notif => (
              <NotificationCard key={notif.id} notif={notif} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
