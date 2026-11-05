import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { contrastRatio, readThemeTokens, resetThemeTokens, type ThemeTokens } from './tokens';

// mermaid itself does not run in jsdom (no layout); the mount is tested
// against a stand-in that records what it is told and returns a fixed SVG.
const fake = vi.hoisted(() => ({
  initialize: vi.fn(),
  parse: vi.fn(async () => ({ diagramType: 'flowchart-v2' })),
  render: vi.fn(async (id: string) => ({
    svg: `<svg id="${id}" xmlns="http://www.w3.org/2000/svg" width="320" height="120" viewBox="0 0 320 120">`
      + `<style>#${id} .node rect{fill:#2d2d2d;}</style><g class="node"><rect width="10" height="10"/></g></svg>`,
  })),
  getDiagramFromText: vi.fn(async (): Promise<{ type: string; db: unknown }> => ({ type: 'flowchart-v2', db: {} })),
}));

vi.mock('mermaid', () => ({
  default: {
    initialize: fake.initialize,
    parse: fake.parse,
    render: fake.render,
    mermaidAPI: { getDiagramFromText: fake.getDiagramFromText },
  },
}));

const { mermaidThemeFor, mountMermaid } = await import('./mermaid');

const HEX = /^#[0-9a-f]{6}$/;

/** Theme variables that are not colours. */
const NOT_COLOURS = new Set([
  'darkMode', 'fontFamily', 'fontSize', 'useGradient', 'dropShadow', 'strokeWidth', 'radius',
  'pieTitleTextSize', 'pieSectionTextSize', 'pieLegendTextSize', 'pieStrokeWidth', 'pieOuterStrokeWidth', 'pieOpacity',
  'commitLabelFontSize', 'tagLabelFontSize',
]);

const themes = ['dark', 'light'] as const;

function tokensFor(theme: 'dark' | 'light'): ThemeTokens {
  resetThemeTokens();
  return readThemeTokens(theme);
}

describe('mermaidThemeFor', () => {
  it.each(themes)('states every colour as hex (%s)', (theme) => {
    const { themeVariables } = mermaidThemeFor(tokensFor(theme));
    expect(Object.keys(themeVariables).length).toBeGreaterThan(150);
    for (const [key, value] of Object.entries(themeVariables)) {
      if (value && typeof value === 'object') {
        for (const [inner, color] of Object.entries(value)) {
          const colors = inner === 'plotColorPalette' ? color.split(',') : [color];
          for (const c of colors) expect(c, `${key}.${inner}`).toMatch(HEX);
        }
      } else if (!NOT_COLOURS.has(key)) {
        expect(value, key).toMatch(HEX);
      }
    }
    expect(themeVariables.darkMode).toBe(theme === 'dark');
    expect(themeVariables.fontFamily).toContain('Inter');
    expect(themeVariables.fontSize).toBe('14px');
    // The neo look's gradient strokes and drop shadows are off.
    expect(themeVariables.useGradient).toBe(false);
    expect(themeVariables.dropShadow).toBe('none');
  });

  it.each(themes)('draws readable nodes, labels and edges (%s)', (theme) => {
    const t = tokensFor(theme);
    const v = mermaidThemeFor(t).themeVariables as Record<string, string>;
    expect(v.mainBkg).not.toBe(t.bg);
    expect(v.mainBkg).toBe(t.surfaceRaised);
    expect(contrastRatio(v.nodeTextColor, v.mainBkg)).toBeGreaterThanOrEqual(4.5);
    expect(contrastRatio(v.textColor, v.edgeLabelBackground)).toBeGreaterThanOrEqual(4.5);
    // A node's outline is what sets it off from the figure: 3:1 at least.
    expect(contrastRatio(v.nodeBorder, v.mainBkg)).toBeGreaterThanOrEqual(3);
    expect(contrastRatio(v.lineColor, t.surface)).toBeGreaterThanOrEqual(3);
    expect(v.clusterBkg).not.toBe(t.surface);
    // Wherever text sits on a tinted fill, it still reads.
    const tinted = [
      'noteBkgColor', 'taskBkgColor', 'activeTaskBkgColor', 'critBkgColor', 'doneTaskBkgColor', 'actorBkg',
      'labelBoxBkgColor', 'compositeBackground', 'quadrant1Fill', 'quadrant2Fill', 'quadrant3Fill', 'quadrant4Fill',
      ...Array.from({ length: 12 }, (_, i) => `cScale${i}`),
      ...Array.from({ length: 8 }, (_, i) => `fillType${i}`),
    ];
    for (const key of tinted) expect(contrastRatio(t.text, v[key]), key).toBeGreaterThanOrEqual(4.5);
  });

  it.each(themes)('takes series, slices and branches from the palette (%s)', (theme) => {
    const t = tokensFor(theme);
    const v = mermaidThemeFor(t).themeVariables as Record<string, string> & { xyChart: Record<string, string> };
    for (let i = 0; i < 12; i++) expect(v[`pie${i + 1}`]).toBe(t.palette[i % t.palette.length]);
    for (let i = 0; i < 8; i++) {
      expect(v[`git${i}`]).toBe(t.palette[i]);
      // A branch name reads on its branch's colour.
      expect(contrastRatio(v[`gitBranchLabel${i}`], v[`git${i}`])).toBeGreaterThanOrEqual(4.5);
    }
    expect(v.xyChart.plotColorPalette).toBe(t.palette.join(','));
    // One slice-label colour that reads on every slice.
    for (let i = 1; i <= 12; i++) expect(contrastRatio(v.pieSectionTextColor, v[`pie${i}`])).toBeGreaterThanOrEqual(4.5);
    // The single-colour charts lead with the first hue, not the accent.
    expect(v.taskBorderColor).not.toBe(t.accent);
    expect(v.quadrantPointFill).toBe(t.palette[0]);
  });

  it('adds shape CSS that loads nothing', () => {
    const { themeCSS } = mermaidThemeFor(tokensFor('dark'));
    expect(themeCSS).toContain('rx: 6px; ry: 6px;');
    expect(themeCSS).toContain('stroke-width: 1.25px');
    expect(themeCSS).toMatch(/\.edgeLabel p \{[^}]*padding/);
    expect(themeCSS).toMatch(/\.task3 \{/);
    expect(themeCSS).not.toMatch(/url\(\s*['"]?(?!#)/);
    expect(themeCSS).not.toContain('@import');
    // Balanced, or mermaid replaces it all with an error comment.
    expect(themeCSS.split('{').length).toBe(themeCSS.split('}').length);
  });

  it('is pure: the same tokens give the same theme', () => {
    const t = tokensFor('light');
    expect(mermaidThemeFor(t)).toEqual(mermaidThemeFor(t));
  });
});

describe('mountMermaid', () => {
  let stage: HTMLDivElement;

  beforeEach(() => {
    stage = document.createElement('div');
    document.body.appendChild(stage);
    fake.initialize.mockClear();
    fake.parse.mockClear();
    fake.render.mockClear();
    fake.getDiagramFromText.mockClear();
  });

  afterEach(() => {
    stage.remove();
    resetThemeTokens();
  });

  /** The config of the latest `initialize` call. */
  const config = () => fake.initialize.mock.calls.at(-1)?.[0] as Record<string, unknown> & {
    themeVariables: Record<string, unknown>;
    flowchart: { useMaxWidth: boolean };
    gantt: { useMaxWidth: boolean; useWidth: number };
    dompurifyConfig: { FORBID_TAGS: string[]; FORBID_ATTR: string[] };
    secure: string[];
  };

  it('initializes the base theme, strict, at natural size, before every render', async () => {
    await mountMermaid(stage, 'flowchart LR\n  A --> B', { theme: 'dark', width: 680 });
    expect(fake.initialize).toHaveBeenCalledTimes(1);
    const c = config();
    expect(c.theme).toBe('base');
    expect(c.securityLevel).toBe('strict');
    expect(c.suppressErrorRendering).toBe(true);
    expect(c.startOnLoad).toBe(false);
    expect(c.flowchart.useMaxWidth).toBe(false);
    for (const key of ['sequence', 'gantt', 'class', 'state', 'er', 'pie', 'xyChart', 'gitGraph', 'mindmap', 'timeline', 'journey', 'c4']) {
      expect((c[key] as { useMaxWidth: boolean }).useMaxWidth, key).toBe(false);
    }
    // A gantt chart spans the column it was asked for, not the page.
    expect(c.gantt.useWidth).toBe(680);
    expect(c.themeVariables).toEqual(mermaidThemeFor(readThemeTokens('dark')).themeVariables);
    expect(c.themeCSS).toBe(mermaidThemeFor(readThemeTokens('dark')).themeCSS);
    expect(c.fontFamily).toContain('Inter');
    // The order mermaid needs: configure, check the syntax, draw.
    expect(fake.initialize.mock.invocationCallOrder[0]).toBeLessThan(fake.parse.mock.invocationCallOrder[0]);
    expect(fake.parse.mock.invocationCallOrder[0]).toBeLessThan(fake.render.mock.invocationCallOrder[0]);

    await mountMermaid(stage, 'flowchart LR\n  A --> B', { theme: 'light', width: 0 });
    expect(fake.initialize).toHaveBeenCalledTimes(2);
    expect(config().themeVariables.darkMode).toBe(false);
    expect(config().gantt.useWidth).toBeGreaterThan(0);
  });

  it('keeps loading things out of labels and stylesheet keys out of diagram directives', async () => {
    await mountMermaid(stage, 'flowchart LR\n  A --> B', { theme: 'dark', width: 680 });
    const c = config();
    for (const tag of ['img', 'image', 'svg', 'iframe', 'video', 'style']) expect(c.dompurifyConfig.FORBID_TAGS).toContain(tag);
    for (const attr of ['style', 'src', 'srcset']) expect(c.dompurifyConfig.FORBID_ATTR).toContain(attr);
    for (const key of ['securityLevel', 'themeCSS', 'fontFamily', 'altFontFamily']) expect(c.secure).toContain(key);
  });

  it('hands back the natural size and a standalone SVG', async () => {
    const mounted = await mountMermaid(stage, 'flowchart LR\n  A --> B', { theme: 'dark', width: 680 });
    expect(mounted.naturalSize).toEqual({ width: 320, height: 120 });
    const svg = stage.querySelector('svg')!;
    expect(svg.getAttribute('width')).toBe('320');
    expect(svg.getAttribute('height')).toBe('120');

    // The frame sizes the picture on screen; the file must not inherit that.
    svg.style.width = '160px';
    const file = await mounted.toSvg!();
    expect(file).toBeTruthy();
    const doc = new DOMParser().parseFromString(file!, 'image/svg+xml');
    expect(doc.querySelector('parsererror')).toBeNull();
    const root = doc.documentElement;
    expect(root.namespaceURI).toBe('http://www.w3.org/2000/svg');
    expect(file).toMatch(/^<svg[^>]* xmlns="http:\/\/www\.w3\.org\/2000\/svg"/);
    expect(root.getAttribute('width')).toBe('320');
    expect(root.getAttribute('height')).toBe('120');
    expect(root.getAttribute('style')).toBeNull();
    // mermaid's stylesheet travels with the picture, scoped by the root's own id.
    const id = root.getAttribute('id')!;
    expect(id).toMatch(/^nerve-mermaid-\d+$/);
    expect(root.querySelector('style')?.textContent).toContain(`#${id} .node rect`);
    // The figure's background, behind everything, so light text stays readable as a file.
    const backdrop = [...root.children].find(child => child.localName === 'rect');
    expect(backdrop?.getAttribute('fill')).toBe(readThemeTokens('dark').surface);
    expect(backdrop?.getAttribute('width')).toBe('320');

    mounted.cleanup();
    expect(stage.innerHTML).toBe('');
  });

  it('sizes a picture mermaid states as width="100%" from its viewBox', async () => {
    fake.render.mockImplementationOnce(async (id: string) => ({
      svg: `<svg id="${id}" width="100%" style="max-width: 700px;" viewBox="0 0 700 400"><g/></svg>`,
    }));
    const mounted = await mountMermaid(stage, 'xychart-beta\n  bar [1, 2]', { theme: 'light', width: 680 });
    expect(mounted.naturalSize).toEqual({ width: 700, height: 400 });
    const svg = stage.querySelector('svg')!;
    expect(svg.getAttribute('width')).toBe('700');
    expect(svg.getAttribute('height')).toBe('400');
    expect(svg.style.maxWidth).toBe('');
  });

  it('takes the height a viewBox implies when the stated one leaves part out', async () => {
    // mermaid's journey diagram states its height without the title it adds.
    fake.render.mockImplementationOnce(async (id: string) => ({
      svg: `<svg id="${id}" width="1100" height="565" viewBox="0 -25 1100 635"><g/></svg>`,
    }));
    const mounted = await mountMermaid(stage, 'journey\n  title A day', { theme: 'dark', width: 680 });
    expect(mounted.naturalSize).toEqual({ width: 1100, height: 635 });
  });

  it('reports a syntax error in two short lines and draws nothing', async () => {
    fake.parse.mockRejectedValueOnce(new Error('Parse error on line 2:\n...A --> ;\n----^\nExpecting \'NODE_STRING\', got \';\''));
    await expect(mountMermaid(stage, 'flowchart LR\n  A --> ;', { theme: 'dark', width: 680 }))
      .rejects.toThrow(/^Parse error on line 2: \.\.\.A --> ;$/);
    expect(fake.render).not.toHaveBeenCalled();
    expect(stage.innerHTML).toBe('');
  });

  it('refuses a diagram that would fetch an image while it is drawn', async () => {
    const vertices = (img: string) => ({ type: 'flowchart-v2', db: { getVertices: () => new Map([['A', { id: 'A', img }]]) } });
    fake.getDiagramFromText.mockResolvedValueOnce(vertices('https://example.invalid/x.png'));
    await expect(mountMermaid(stage, 'flowchart LR\n  A@{ img: "…" }', { theme: 'dark', width: 680 }))
      .rejects.toThrow(/example\.invalid.*load nothing from the network/);
    expect(fake.render).not.toHaveBeenCalled();

    const actors = (icon: string) => ({ type: 'sequence', db: { getActors: () => new Map([['A', { properties: { icon } }]]) } });
    fake.getDiagramFromText.mockResolvedValueOnce(actors('//example.invalid/icon.png'));
    await expect(mountMermaid(stage, 'sequenceDiagram\n  participant A', { theme: 'dark', width: 680 }))
      .rejects.toThrow(/load nothing from the network/);
    expect(fake.render).not.toHaveBeenCalled();

    // Pictures that carry their own pixels, and mermaid's built-in symbols, are fine.
    fake.getDiagramFromText.mockResolvedValueOnce(vertices('data:image/png;base64,iVBORw0KGgo='));
    await mountMermaid(stage, 'flowchart LR\n  A@{ img: "…" }', { theme: 'dark', width: 680 });
    fake.getDiagramFromText.mockResolvedValueOnce(actors('@clock'));
    await mountMermaid(stage, 'sequenceDiagram\n  participant A', { theme: 'dark', width: 680 });
    expect(fake.render).toHaveBeenCalledTimes(2);
  });

  it('refuses styles that would fetch while mermaid draws', async () => {
    const refused = [
      'flowchart LR\n  A --> B\n  style A fill:url(https://example.invalid/a.svg#g)',
      'flowchart LR\n  A["see url(//example.invalid/b.png)"] --> B',
      // \\75 is "u": the escape spells url( without its letters.
      'stateDiagram-v2\n  [*] --> A\n  classDef x background-image:\\75 rl(\\2f \\2f example.invalid\\2f c.png)\n  class A x',
      'sequenceDiagram\n  rect url(//example.invalid/d.svg#g)\n  A->>B: hi\n  end',
    ];
    for (const source of refused) {
      await expect(mountMermaid(stage, source, { theme: 'dark', width: 680 })).rejects.toThrow(/load nothing from the network/);
    }
    expect(fake.parse).not.toHaveBeenCalled();
    expect(fake.render).not.toHaveBeenCalled();

    // A fragment reference stays inside the picture; a backslash in a label is only text.
    await mountMermaid(stage, 'flowchart LR\n  A["C:\\Users\\Admin"] --> B\n  style B fill:url(#pattern)', { theme: 'dark', width: 680 });
    expect(fake.render).toHaveBeenCalledTimes(1);
  });

  it('puts nothing on the page that would fetch', async () => {
    fake.render.mockImplementationOnce(async (id: string) => ({
      svg: `<svg id="${id}" width="200" height="100" viewBox="0 0 200 100">`
        + `<style>#${id} .a{fill:url(#grad)} #${id} .b{background:url("https://example.invalid/d.png")} @import url(https://example.invalid/e.css);</style>`
        + '<defs><linearGradient id="grad"><stop offset="0"/></linearGradient></defs>'
        + '<foreignObject width="100" height="20"><div><img src="https://example.invalid/a.png">'
        + '<span style="color: red; background: url(https://example.invalid/b.png)">label</span></div></foreignObject>'
        + '<image href="https://example.invalid/c.png" width="10" height="10"/>'
        + '<image href="data:image/png;base64,iVBORw0KGgo=" width="10" height="10"/>'
        + '<use href="#grad"/>'
        + '<a href="https://example.invalid/link"><text>link</text></a>'
        // A CSS escape spells url( without its letters: \75 is "u", \2f is "/".
        + '<rect class="x" style="background-image:\\75 rl(\\2f \\2f example.invalid\\2f v.png) !important"/>'
        + `<style>#${id} .x span { background-image: \\75 rl(\\2f \\2f example.invalid\\2f w.png) }</style>`
        + '</svg>',
    }));
    await mountMermaid(stage, 'flowchart LR\n  A --> B', { theme: 'dark', width: 680 });
    const html = stage.innerHTML;
    // No reference out of the picture is left, in any spelling.
    expect(html).not.toMatch(/url\(\s*['"]?\s*(?!#)/i);
    expect(html).not.toContain('\\');
    expect(html).not.toContain('@import');
    expect(stage.querySelector('img')).toBeNull();
    expect(stage.querySelector('image[href^="http"]')).toBeNull();
    // A style that reaches out goes whole; the stylesheet keeps the rest.
    expect(stage.querySelector('span')?.hasAttribute('style')).toBe(false);
    expect(stage.querySelector('rect.x')?.hasAttribute('style')).toBe(false);
    expect(html).toContain('url(#grad)');
    // The link is left: following it takes a click.
    expect(html).toContain('href="https://example.invalid/link"');
    expect(html).toContain('data:image/png;base64,iVBORw0KGgo=');
    expect(html).toContain('<use href="#grad"');
    expect(html).toContain('label');
  });

  it('draws one diagram at a time, each with its own settings', async () => {
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    fake.render.mockImplementationOnce(async (id: string) => {
      await held;
      return { svg: `<svg id="${id}" width="10" height="10"></svg>` };
    });
    const first = mountMermaid(stage, 'flowchart LR\n  A --> B', { theme: 'dark', width: 680 });
    const other = document.createElement('div');
    const second = mountMermaid(other, 'flowchart LR\n  C --> D', { theme: 'light', width: 680 });
    await vi.waitFor(() => expect(fake.render).toHaveBeenCalledTimes(1));
    // The second render has not reset the configuration under the first one.
    expect(fake.initialize).toHaveBeenCalledTimes(1);
    release();
    await Promise.all([first, second]);
    expect(fake.initialize).toHaveBeenCalledTimes(2);
    expect(fake.initialize.mock.invocationCallOrder[1]).toBeGreaterThan(fake.render.mock.invocationCallOrder[0]);
    expect(config().themeVariables.darkMode).toBe(false);
  });
});
