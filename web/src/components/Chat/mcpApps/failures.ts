import { ApiError } from '../../../api/hosted';

/**
 * Why a request for a view failed, in words for the person (the block) and
 * for the view (the error of its tool call).
 */
export interface Failure {
  /**
   * The session has no live runtime to ask (409 `session_not_running`): the
   * session is asleep, not broken, and a message to it wakes it.
   */
  asleep: boolean;
  text: string;
}

/** The gateway's code for a session without a live runtime. */
const SESSION_NOT_RUNNING = 'session_not_running';
const MAX_TEXT = 300;

/** The gateway's codes that a person can meet in front of a view, in words. */
const IN_WORDS: Record<string, string> = {
  // An old chat whose MCP server was taken out of the configuration since.
  server_not_found: 'Nerve has no MCP server of that name. It may have been taken out of the configuration.',
  resource_not_found: 'The server does not have this view.',
  tool_not_found: 'The server does not have this tool.',
  tool_not_app_visible: 'The server does not let views call this tool.',
  mcp_apps_disabled: 'MCP Apps is turned off on this Nerve.',
};

/** The `detail` of a gateway error body, from the `"<status>: <body>"` message of an `ApiError`. */
function detailOf(error: ApiError): string {
  const body = error.message.slice(error.message.indexOf(':') + 1).trim();
  try {
    const parsed: unknown = JSON.parse(body);
    const detail = parsed && typeof parsed === 'object' && 'detail' in parsed ? parsed.detail : null;
    if (typeof detail === 'string') return detail;
    if (detail && typeof detail === 'object' && 'message' in detail && typeof detail.message === 'string') {
      return detail.message;
    }
  } catch {
    // Not JSON: a proxy's error page, or no body at all.
  }
  return '';
}

export function failureOf(error: unknown): Failure {
  if (error instanceof ApiError) {
    const detail = detailOf(error);
    if (error.status === 409 && detail === SESSION_NOT_RUNNING) {
      return { asleep: true, text: 'The session is asleep.' };
    }
    return {
      asleep: false,
      text: detail ? (IN_WORDS[detail] ?? detail.slice(0, MAX_TEXT)) : `The request failed (${error.status}).`,
    };
  }
  if (error instanceof Error) {
    if (error.name === 'TimeoutError' || error.name === 'AbortError') {
      return { asleep: false, text: 'Nerve did not answer in time.' };
    }
    // `fetch` rejects with a TypeError when nothing answers.
    if (error instanceof TypeError) return { asleep: false, text: 'Nerve could not be reached.' };
    return { asleep: false, text: error.message.slice(0, MAX_TEXT) || 'The request failed.' };
  }
  return { asleep: false, text: 'The request failed.' };
}
