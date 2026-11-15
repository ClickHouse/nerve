import { useEffect, useState } from 'react';

/**
 * `value` once it has stopped changing for `delayMs`; with a delay of 0 it is
 * `value` itself. A streamed code block changes with every token batch, and a
 * diagram renderer run on each of them would mostly see half a diagram, so a
 * rendering block waits for the text to settle.
 */
export function useSettledValue<T>(value: T, delayMs: number): T {
  const [settled, setSettled] = useState(value);
  useEffect(() => {
    // With no delay the value is returned as is; the state still follows it
    // (on the next tick), so a delay that starts later starts from it.
    const timer = setTimeout(() => setSettled(value), Math.max(0, delayMs));
    return () => clearTimeout(timer);
  }, [value, delayMs]);
  return delayMs <= 0 ? value : settled;
}
