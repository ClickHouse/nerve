import { afterEach, describe, expect, it } from 'vitest';
import { DEFAULT_PREVIEW_MAX_BYTES, applyViewerPreview, resetPreviewConfig, usePreviewStore } from './previewStore';

afterEach(() => resetPreviewConfig());

describe('the switch of the Preview tab', () => {
  it('is off without a preview member in the viewer', () => {
    applyViewerPreview(undefined);
    expect(usePreviewStore.getState().enabled).toBe(false);
  });

  it('is on with it, with the limit of the server', () => {
    applyViewerPreview({ max_bytes: 2048 });
    expect(usePreviewStore.getState()).toMatchObject({ enabled: true, maxBytes: 2048 });
  });

  it('keeps the default limit for a value out of range', () => {
    applyViewerPreview({ max_bytes: -1 });
    expect(usePreviewStore.getState()).toMatchObject({ enabled: true, maxBytes: DEFAULT_PREVIEW_MAX_BYTES });
    applyViewerPreview(null);
    expect(usePreviewStore.getState().enabled).toBe(false);
  });
});
