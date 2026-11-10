import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { MessageContent } from '../MessageContent';
import { getRendererType } from './index';

describe('Linear records', () => {
  it('render with the Linear renderer', () => {
    expect(getRendererType('linear')).toBe('linear');
  });

  it('show the issue card and link to the subject', () => {
    render(
      <MessageContent
        source="linear"
        summary="[ENG-12] Alice commented: Fix the login"
        content="Issue: ENG-12 Fix the login"
        metadata={{
          identifier: 'ENG-12', team_key: 'ENG', event: 'commented', state: 'In Progress',
          subject_url: 'https://linear.app/acme/issue/ENG-12#comment-c1',
        }}
      />,
    );
    expect(screen.getByText('ENG-12')).toBeTruthy();
    expect(screen.getByText('commented')).toBeTruthy();
    expect(screen.getByText('In Progress')).toBeTruthy();
    const link = screen.getByRole('link', { name: /View in Linear/ });
    expect(link.getAttribute('href')).toBe('https://linear.app/acme/issue/ENG-12#comment-c1');
  });
});
