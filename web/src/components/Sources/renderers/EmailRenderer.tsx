import { useState, useRef, useEffect } from 'react';
import { Button } from '../../ui';
import { MarkdownRenderer } from './MarkdownRenderer';

interface Props {
  content: string;
  rawContent: string | null;
  summary: string;
}

export function EmailRenderer({ content, rawContent, summary }: Props) {
  const [showHtml, setShowHtml] = useState(!!rawContent);
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const [iframeHeight, setIframeHeight] = useState(400);

  // Auto-resize iframe to content height
  useEffect(() => {
    if (!showHtml || !iframeRef.current) return;

    const resize = () => {
      const iframe = iframeRef.current;
      if (!iframe) return;
      try {
        const height = iframe.contentDocument?.documentElement?.scrollHeight;
        if (height && height > 100) {
          setIframeHeight(Math.min(height + 32, 2000));
        }
      } catch {
        // cross-origin safety — ignore
      }
    };

    // Resize after load and on subsequent renders
    const iframe = iframeRef.current;
    iframe.addEventListener('load', resize);
    return () => iframe.removeEventListener('load', resize);
  }, [showHtml]);

  if (!rawContent) {
    return <MarkdownRenderer content={content} />;
  }

  // Base styles for the sandboxed document, deliberately light in BOTH themes.
  //
  // Not a token oversight: HTML email is authored against a white background,
  // and a large share of it sets foreground colours (dark body text, dark table
  // borders) without setting a background. Painting this document dark would
  // leave that mail dark-on-dark and unreadable, which is worse than a light
  // panel inside a dark page. The iframe is the isolation boundary that makes
  // the choice safe: nothing here leaks into the app's palette, and the frame
  // itself carries the app's border and radius.
  //
  // Consequence to keep in mind: `bg-white` on the frame below is load-bearing
  // for the same reason — it stops a transparent-background email from
  // rendering its dark text straight onto the app's dark surface.
  const styledHtml = `<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  font-size: 14px;
  line-height: 1.6;
  color: #222;
  background: #fff;
  padding: 16px;
  margin: 0;
  word-wrap: break-word;
  overflow-wrap: break-word;
}
a { color: #4f46e5; }
img { max-width: 100%; height: auto; }
table { border-collapse: collapse; max-width: 100%; }
td, th { padding: 4px 8px; }
pre { white-space: pre-wrap; }
</style></head>
<body>${rawContent}</body></html>`;

  return (
    <div>
      <div className="flex items-center gap-1 mb-3">
        <Button variant="pill" size="xs" active={showHtml} onClick={() => setShowHtml(true)}>
          HTML
        </Button>
        <Button variant="pill" size="xs" active={!showHtml} onClick={() => setShowHtml(false)}>
          Text
        </Button>
      </div>

      {showHtml ? (
        <iframe
          ref={iframeRef}
          srcDoc={styledHtml}
          sandbox="allow-same-origin"
          className="w-full border border-border-subtle rounded-lg bg-white"
          style={{ height: `${iframeHeight}px` }}
          title={summary}
        />
      ) : (
        <MarkdownRenderer content={content} />
      )}
    </div>
  );
}
