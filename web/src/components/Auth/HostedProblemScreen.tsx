import type { HostedProblem } from '../../api/hosted';
import { Button } from '../ui';

const MESSAGES: Record<HostedProblem, { title: string; text: string }> = {
  access_denied: {
    title: 'No access',
    text: 'You do not have access to this agent. Ask an administrator to give you access.',
  },
  agent_archived: {
    title: 'Agent archived',
    text: 'This agent is archived. You cannot use it.',
  },
  unavailable: {
    title: 'Agent unavailable',
    text: 'This agent cannot be reached at this time.',
  },
};

/**
 * Why hosted Nerve cannot continue, from a gateway answer.
 *
 * `access_denied` and `agent_archived` replace the app, because nothing in it
 * can work. `unavailable` is temporary. Over a mounted app it is a dialog, so
 * the composer text stays. "Try again" calls `onRetry`.
 */
export function HostedProblemScreen({ problem, overlay = false, onRetry }: {
  problem: HostedProblem;
  overlay?: boolean;
  onRetry?: () => void;
}) {
  const { title, text } = MESSAGES[problem];
  const panel = (
    <div className="bg-surface-raised p-8 rounded-lg border border-border-subtle w-80 shadow-xl">
      <h1 id="hosted-problem-title" className="text-xl font-semibold mb-2 text-center">
        {title}
      </h1>
      <p id="hosted-problem-text" className="text-sm text-text-muted text-center">{text}</p>
      {onRetry && (
        <Button
          variant="primary"
          size="md"
          fullWidth
          onClick={onRetry}
          autoFocus={overlay}
          className="mt-6"
        >
          Try again
        </Button>
      )}
    </div>
  );

  if (overlay) {
    return (
      <div
        className="fixed inset-0 z-50 flex items-center justify-center bg-bg/80 backdrop-blur-sm"
        role="dialog"
        aria-modal="true"
        aria-labelledby="hosted-problem-title"
        aria-describedby="hosted-problem-text"
      >
        {panel}
      </div>
    );
  }
  return (
    <main
      className="min-h-screen flex items-center justify-center bg-bg"
      aria-labelledby="hosted-problem-title"
      aria-describedby="hosted-problem-text"
    >
      {panel}
    </main>
  );
}
