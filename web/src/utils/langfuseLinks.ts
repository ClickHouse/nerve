/**
 * Links into the Langfuse web UI, built from `/api/observability/status`.
 *
 * Langfuse UI routes start with `/project/<projectId>`. The backend gets the
 * project id from its startup key check, so it is null when that check could
 * not reach the host. With no project id there is no valid session link.
 */
export interface LangfuseLinkStatus {
  enabled: boolean;
  host: string | null;
  project_id: string | null;
}

function projectBase(status: LangfuseLinkStatus | null | undefined): string | null {
  if (!status?.enabled || !status.host || !status.project_id) return null;
  return `${status.host.replace(/\/+$/, '')}/project/${encodeURIComponent(status.project_id)}`;
}

/** The Langfuse session page for a Nerve session, or null. */
export function langfuseSessionUrl(
  status: LangfuseLinkStatus | null | undefined,
  sessionId: string | null | undefined,
): string | null {
  const base = projectBase(status);
  if (!base || !sessionId) return null;
  return `${base}/sessions/${encodeURIComponent(sessionId)}`;
}

/** The Langfuse project home, or the bare host when the project is unknown. */
export function langfuseProjectUrl(status: LangfuseLinkStatus | null | undefined): string | null {
  return projectBase(status) ?? status?.host ?? null;
}
