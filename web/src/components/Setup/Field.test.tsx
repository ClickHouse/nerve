import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { TextField } from '../ui';
import { Field } from './Field';

describe('Field', () => {
  it('associates the label with the control', () => {
    render(<Field label="Time zone"><TextField defaultValue="UTC" /></Field>);
    expect(screen.getByLabelText('Time zone')).toHaveValue('UTC');
  });

  it('ties help and error text to the control', () => {
    render(
      <Field label="Time zone" help="IANA name" error="Not a time zone name.">
        <TextField defaultValue="x" />
      </Field>,
    );
    const input = screen.getByLabelText('Time zone');
    expect(input).toHaveAttribute('aria-invalid', 'true');
    expect(input).toHaveAccessibleDescription('IANA name Not a time zone name.');
  });

  it('is valid with no error', () => {
    render(<Field label="Name" help="Just a name"><TextField /></Field>);
    const input = screen.getByLabelText('Name');
    expect(input).not.toHaveAttribute('aria-invalid');
    expect(input).toHaveAccessibleDescription('Just a name');
  });
});
