/**
 * Published artifacts: what `GET /api/auth/me` says of them, and what
 * `POST /api/artifacts/publish` answers (`nerve/gateway/routes/artifacts.py`).
 */

/**
 * The `artifacts` member of `GET /api/auth/me`: there only in hosted mode.
 */
export interface ViewerArtifacts {
  /** The Preview tab has the Publish button (`artifacts.publish`). */
  publish: boolean;
  /** The origin of the artifact viewer (`artifacts.viewer_origin`), or null. */
  viewer_origin: string | null;
}

/** `POST /api/artifacts/publish`: a complete version of an artifact. */
export interface PublishedArtifact {
  artifact_id: string;
  version: number;
  /** The viewer address of the artifact, for people. */
  viewer_url: string;
  expires_at: string;
  title: string;
  /** A file changed while it was published: this version has the earlier state. */
  changed_after?: boolean;
}
