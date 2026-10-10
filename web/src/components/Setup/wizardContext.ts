import { createContext, useContext } from 'react';
import type { WizardStop } from './navigation';

/**
 * What a step form needs from the shell around it: where to go next, where
 * back is, and who is looking (so forms can be keyed per viewer).
 */
export interface WizardNav {
  /** Go to a stop (or the Overview with `null`). */
  goTo: (stop: WizardStop | null) => void;
  /** Leave `from` for the next non-blocked stop — or for `returnTo`. */
  advance: (from: WizardStop) => void;
  /** Leave `from` for the previous non-blocked stop, or the Overview — or for `returnTo`. */
  back: (from: WizardStop) => void;
  /**
   * Where this step was opened from, when it was opened to *edit* it (a
   * "Change" or "Open" link on the Overview or the Review carries it in the
   * navigation state). The primary then reads Save and both Save and Back
   * go there, instead of marching on to the next step. `null` in the
   * forward flow and for rail links.
   */
  returnTo: string | null;
  viewerId: string;
}

export const WizardContext = createContext<WizardNav>({
  goTo: () => {},
  advance: () => {},
  back: () => {},
  returnTo: null,
  viewerId: 'nobody',
});

export function useWizardNav(): WizardNav {
  return useContext(WizardContext);
}
