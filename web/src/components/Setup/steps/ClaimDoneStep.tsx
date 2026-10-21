import type { SetupClaimDetail, SetupStep } from '../../../api/client';
import { Link } from 'react-router-dom';
import { STEP_HEADINGS } from '../copy';
import { Notice } from '../Notice';
import { StepFrame } from '../StepFrame';

/**
 * The claim step as seen *after* the claim: read-only, with where to change
 * the password. (The claim form itself renders before sign-in; see
 * `ClaimForm`.) Local mode only — the step does not exist in external mode.
 * Titled "Account" here and in the rail/Overview/Review (`stepTitle`).
 */
export function ClaimDoneStep({ step }: { step: SetupStep }) {
  const detail = step.detail as SetupClaimDetail;
  return (
    <StepFrame
      step={step}
      heading={STEP_HEADINGS.claim.heading}
      intro={STEP_HEADINGS.claim.intro}
      readOnly
      onSubmit={() => {}}
    >
      <Notice tone="info">
        {detail.username ? <>Signed in as <strong>{detail.username}</strong>. </> : null}
        {detail.passwordless ? 'This instance has no password. ' : 'A password is set. '}
        Manage people and passwords in <Link to="/accounts" className="underline">Accounts</Link>.
      </Notice>
    </StepFrame>
  );
}
