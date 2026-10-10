import { useHostedStore } from '../api/hosted';
import { useAuthStore } from '../stores/authStore';
import { ClaimForm } from '../components/Setup/steps/ClaimForm';
import { WizardShell } from '../components/Setup/WizardShell';

/**
 * `/setup`: the first-account claim while an instance is unclaimed (local
 * mode only — external mode has no local accounts and no claim), otherwise
 * the setup wizard, in both auth modes. The claim form is mounted outside
 * the app shell by `App`; the wizard renders inside it.
 */
export function SetupPage() {
  const loginMode = useAuthStore((state) => state.loginMode);
  const hosted = useHostedStore((s) => s.mode === 'external');
  if (!hosted && loginMode === 'setup') return <ClaimForm />;
  return <WizardShell />;
}
