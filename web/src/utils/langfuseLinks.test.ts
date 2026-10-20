import { describe, it, expect } from 'vitest';
import { langfuseProjectUrl, langfuseSessionUrl } from './langfuseLinks';

const status = {
  enabled: true,
  host: 'https://langfuse.example.com',
  project_id: 'proj-1',
};

describe('langfuseSessionUrl', () => {
  it('links to the session page inside the project', () => {
    expect(langfuseSessionUrl(status, 'session-a'))
      .toBe('https://langfuse.example.com/project/proj-1/sessions/session-a');
  });

  it('encodes the session id', () => {
    expect(langfuseSessionUrl(status, 'a b/c'))
      .toBe('https://langfuse.example.com/project/proj-1/sessions/a%20b%2Fc');
  });

  it('ignores a trailing slash on the host', () => {
    expect(langfuseSessionUrl({ ...status, host: 'https://langfuse.example.com/' }, 's'))
      .toBe('https://langfuse.example.com/project/proj-1/sessions/s');
  });

  it('has no link without a project id, a host, a session, or when disabled', () => {
    expect(langfuseSessionUrl({ ...status, project_id: null }, 's')).toBeNull();
    expect(langfuseSessionUrl({ ...status, host: null }, 's')).toBeNull();
    expect(langfuseSessionUrl({ ...status, enabled: false }, 's')).toBeNull();
    expect(langfuseSessionUrl(status, null)).toBeNull();
    expect(langfuseSessionUrl(null, 's')).toBeNull();
  });
});

describe('langfuseProjectUrl', () => {
  it('links to the project when its id is known', () => {
    expect(langfuseProjectUrl(status)).toBe('https://langfuse.example.com/project/proj-1');
  });

  it('falls back to the host when the project id is unknown', () => {
    expect(langfuseProjectUrl({ ...status, project_id: null }))
      .toBe('https://langfuse.example.com');
  });

  it('has no link without a host', () => {
    expect(langfuseProjectUrl({ ...status, host: null, project_id: null })).toBeNull();
    expect(langfuseProjectUrl(undefined)).toBeNull();
  });
});
