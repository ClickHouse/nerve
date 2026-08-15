import { Badge } from '../../ui';
import { Github, ExternalLink } from '../../ui/icons';
import { MarkdownRenderer } from './MarkdownRenderer';

interface Props {
  content: string;
  metadata?: Record<string, any>;
  summary: string;
}

export function GitHubRenderer({ content, metadata, summary: _summary }: Props) {
  void _summary; // Part of common renderer interface
  const repoName = metadata?.repo_name || '';
  const subjectUrl = metadata?.subject_url || '';
  const subjectType = metadata?.subject_type || '';
  const reason = metadata?.reason || '';

  return (
    <div>
      {/* GitHub header card */}
      {(repoName || subjectUrl) && (
        <div className="mb-4 p-3 bg-surface border border-border-subtle rounded-lg">
          <div className="flex items-center gap-2 mb-1 flex-wrap">
            <Github size={14} className="text-hue-purple shrink-0" />
            <span className="text-sm text-text-secondary font-medium">{repoName}</span>
            {/* Purple is GitHub's identity here, not a status — it matches the
                `text-hue-purple` mark above it, and `hue-purple` follows the
                light theme where the stock `purple-500/15` did not. */}
            {subjectType && <Badge tone="purple">{subjectType}</Badge>}
            {reason && <Badge>{reason}</Badge>}
          </div>
          {subjectUrl && (
            <a href={subjectUrl} target="_blank" rel="noopener noreferrer"
              className="flex items-center gap-1 text-xs text-accent hover:text-link transition-colors">
              <ExternalLink size={11} /> View on GitHub
            </a>
          )}
        </div>
      )}

      <MarkdownRenderer content={content} />
    </div>
  );
}
