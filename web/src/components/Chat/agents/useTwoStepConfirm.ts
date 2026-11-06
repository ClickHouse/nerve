import { useEffect, useRef, useState } from 'react';

/**
 * A destructive action that takes two presses: the first
 * press arms it for `windowMs`, a second press inside the window runs it, and
 * the arming lapses on its own after the window.
 */
export function useTwoStepConfirm(windowMs: number): {
  armed: boolean;
  /** Arm, or run `action` when already armed. */
  press: (action: () => void) => void;
  disarm: () => void;
} {
  const [armed, setArmed] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => () => {
    if (timer.current !== null) clearTimeout(timer.current);
  }, []);

  const disarm = () => {
    if (timer.current !== null) clearTimeout(timer.current);
    timer.current = null;
    setArmed(false);
  };

  const press = (action: () => void) => {
    if (armed) {
      disarm();
      action();
      return;
    }
    if (timer.current !== null) clearTimeout(timer.current);
    setArmed(true);
    timer.current = setTimeout(() => {
      timer.current = null;
      setArmed(false);
    }, windowMs);
  };

  return { armed, press, disarm };
}
