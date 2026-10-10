import { useState, type ReactNode } from 'react';
import { Button } from '../ui';
import { Check, CircleX, Loader2 } from '../ui/icons';
import { errorDetail } from '../../stores/accountStore';

export interface TestOutcome {
  ok: boolean;
  detail: string | null;
  hint?: string | null;
}

/**
 * Runs one server-side check and reports the result beside the button.
 *
 * The check runs on the *server* — its network, its credentials — which is
 * why the result says precisely what failed there. The result is a
 * `role="status"` region so it is announced without stealing focus.
 */
export function TestButton({ label = 'Test connection', run, disabled, onResult, children }: {
  label?: string;
  run: () => Promise<TestOutcome>;
  disabled?: boolean;
  onResult?: (outcome: TestOutcome) => void;
  /** Extra content rendered after a successful result (e.g. a model picker). */
  children?: ReactNode;
}) {
  const [busy, setBusy] = useState(false);
  const [outcome, setOutcome] = useState<TestOutcome | null>(null);

  const test = async () => {
    setBusy(true);
    setOutcome(null);
    let result: TestOutcome;
    try {
      result = await run();
    } catch (e) {
      result = { ok: false, detail: errorDetail(e, 'The check could not run.') };
    }
    setOutcome(result);
    setBusy(false);
    onResult?.(result);
  };

  return (
    <div className="flex flex-col gap-2">
      <div>
        <Button variant="secondary" size="sm" onClick={() => void test()} disabled={disabled || busy} className="min-h-[44px] md:min-h-0">
          {busy ? <Loader2 size={14} className="animate-spin" /> : null}
          {busy ? 'Testing…' : label}
        </Button>
      </div>
      <div role="status" aria-live="polite">
        {outcome && (
          <div className={`flex items-start gap-2 rounded-lg border p-2 text-sm
            ${outcome.ok ? 'border-success-border bg-success-bg text-success' : 'border-error-border bg-error-bg text-error'}`}>
            {outcome.ok ? <Check size={14} className="mt-0.5 shrink-0" /> : <CircleX size={14} className="mt-0.5 shrink-0" />}
            <div className="min-w-0">
              <p>{outcome.detail ?? (outcome.ok ? 'Works.' : 'Failed.')}</p>
              {outcome.hint && <p className="text-xs opacity-80 mt-0.5">{outcome.hint}</p>}
            </div>
          </div>
        )}
      </div>
      {outcome?.ok && children}
    </div>
  );
}
