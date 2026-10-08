import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { ContextBar } from './ContextBar';

// A turn with three calls whose inputs grow: 100k, 140k and 180k.
const usage = {
  input_tokens: 200_000,
  output_tokens: 1_500,
  cache_creation_input_tokens: 0,
  cache_read_input_tokens: 220_000,
  max_context_tokens: 272_000,
  num_turns: 3,
};

describe('ContextBar', () => {
  it('shows the input of the last call when the backend reports it', () => {
    render(<ContextBar usage={{ ...usage, context_tokens: 180_000 }} />);
    expect(screen.getByText('~180.0k / 272.0k')).toBeInTheDocument();
  });

  it('divides the turn input by the number of calls otherwise', () => {
    render(<ContextBar usage={usage} />);
    expect(screen.getByText('~140.0k / 272.0k')).toBeInTheDocument();
  });
});
