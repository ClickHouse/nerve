import type { SetupStep } from '../../../api/client';
import { NOT_AVAILABLE_HERE, STEP_HEADINGS } from '../copy';
import { StepFrame } from '../StepFrame';

/**
 * A step this instance has closed: it keeps no credentials on disk and the
 * step's only purpose is to store one (`blocked_kind: policy`). There is no
 * form to fill in — the frame shows the server's reason and offers Next.
 */
export function UnavailableStep({ step }: { step: SetupStep }) {
  return (
    <StepFrame
      step={step}
      heading={STEP_HEADINGS[step.id].heading}
      intro={`${NOT_AVAILABLE_HERE}.`}
      onSubmit={() => undefined}
      readOnly
    >
      {null}
    </StepFrame>
  );
}
