import { describe, expect, it } from 'vitest';
import { ApiError } from '../../../api/hosted';
import { failureOf } from './failures';

describe('failureOf', () => {
  it('reads 409 session_not_running as a session that is asleep', () => {
    expect(failureOf(new ApiError(409, '{"detail":"session_not_running"}')))
      .toEqual({ asleep: true, text: 'The session is asleep.' });
  });

  it('reads any other 409 as a failure', () => {
    expect(failureOf(new ApiError(409, '{"detail":"conflict"}'))).toEqual({ asleep: false, text: 'conflict' });
  });

  it('gives the detail of a gateway error', () => {
    expect(failureOf(new ApiError(502, '{"detail":"the server closed the connection"}')))
      .toEqual({ asleep: false, text: 'the server closed the connection' });
    expect(failureOf(new ApiError(502, '{"detail":"some_code_of_tomorrow"}')).text).toBe('some_code_of_tomorrow');
    expect(failureOf(new ApiError(404, '{"detail":{"code":"unknown","message":"no such resource"}}')).text).toBe('no such resource');
  });

  it.each([
    ['server_not_found', 404, 'Nerve has no MCP server of that name. It may have been taken out of the configuration.'],
    ['resource_not_found', 404, 'The server does not have this view.'],
    ['tool_not_found', 404, 'The server does not have this tool.'],
    ['tool_not_app_visible', 403, 'The server does not let views call this tool.'],
    ['mcp_apps_disabled', 404, 'MCP Apps is turned off on this Nerve.'],
  ])('says the gateway\'s code %s in words', (code, status, words) => {
    expect(failureOf(new ApiError(status, JSON.stringify({ detail: code })))).toEqual({ asleep: false, text: words });
  });

  it('gives the status when the body says nothing', () => {
    expect(failureOf(new ApiError(504, '')).text).toBe('The request failed (504).');
    expect(failureOf(new ApiError(502, '<html>Bad Gateway</html>')).text).toBe('The request failed (502).');
    expect(failureOf(new ApiError(422, '{"detail":[{"loc":["query","uri"]}]}')).text).toBe('The request failed (422).');
  });

  it('cuts a long detail', () => {
    expect(failureOf(new ApiError(502, JSON.stringify({ detail: 'x'.repeat(5000) }))).text).toHaveLength(300);
  });

  it('names a deadline and an unreachable gateway', () => {
    const timeout = new Error('signal timed out');
    timeout.name = 'TimeoutError';
    expect(failureOf(timeout).text).toBe('Nerve did not answer in time.');
    expect(failureOf(new TypeError('Failed to fetch')).text).toBe('Nerve could not be reached.');
  });

  it('gives the message of any other error, and a word for what is no error', () => {
    expect(failureOf(new Error('The app has no document.'))).toEqual({ asleep: false, text: 'The app has no document.' });
    expect(failureOf('nope').text).toBe('The request failed.');
    expect(failureOf(undefined).text).toBe('The request failed.');
  });
});
