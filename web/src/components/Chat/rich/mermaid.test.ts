import DOMPurify from 'dompurify';
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
          // A size, at the 11px floor or above; everything else is a colour.
          if (/FontSize$/.test(inner)) {
            expect(parseFloat(color), `${key}.${inner}`).toBeGreaterThanOrEqual(11);
            continue;
          }
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
    gantt: { useMaxWidth: boolean; useWidth: number; axisFormat: string };
    xyChart: { useMaxWidth: boolean; width: number; height: number };
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
    // A gantt chart and an xychart span the column they were asked for: no
    // page-wide drawing, no 700px one shrunk (small text and all) to fit.
    expect(c.gantt.useWidth).toBe(680);
    expect(c.xyChart.width).toBe(680);
    expect(c.xyChart.height).toBeGreaterThanOrEqual(240);
    expect(c.xyChart.height).toBeLessThanOrEqual(400);
    // A date format short enough for 12px axis labels.
    expect(c.gantt.axisFormat).toBe('%b %-d');
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
    expect(config().xyChart.width).toBeGreaterThan(0);
  });

  it('keeps loading things out of labels and stylesheet keys out of diagram directives', async () => {
    await mountMermaid(stage, 'flowchart LR\n  A --> B', { theme: 'dark', width: 680 });
    const c = config();
    for (const tag of ['img', 'image', 'feImage', 'use', 'foreignObject', 'iframe', 'video', 'style']) {
      expect(c.dompurifyConfig.FORBID_TAGS).toContain(tag);
    }
    for (const attr of ['src', 'srcset']) expect(c.dompurifyConfig.FORBID_ATTR).toContain(attr);
    for (const key of ['securityLevel', 'themeCSS', 'fontFamily', 'altFontFamily']) expect(c.secure).toContain(key);

    // mermaid sanitizes label HTML, and its own icons, with these settings on
    // DOMPurify's default instance: an icon (inline svg, styled) survives,
    // anything that reaches out of the picture does not, however spelled.
    const clean = DOMPurify.sanitize(
      '<svg width="8" height="8"><rect width="8" height="8" style="fill: #087ebf" fill="url(#g)"/>'
      + '<rect fill="url(https://example.invalid/p.svg#p)"/><rect mask="\\75 rl(\\2f \\2f example.invalid\\2f m)"/>'
      + '<image href="https://example.invalid/i.png"/><use href="https://example.invalid/u.svg#u"/></svg>'
      + '<span style="color: red">ok</span><span style="background: url(https://example.invalid/b.png)">bg</span>',
      c.dompurifyConfig,
    );
    expect(clean).not.toContain('example.invalid');
    expect(clean).toContain('<svg');
    expect(clean).toContain('fill: #087ebf');
    expect(clean).toContain('fill="url(#g)"');
    expect(clean).toContain('style="color: red"');
    expect(clean).not.toContain('<image');
    expect(clean).not.toContain('<use');
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

    mounted.cleanup();
    expect(stage.innerHTML).toBe('');
  });

  it('exports every part of the picture, adding nothing over it', async () => {
    // A sequence diagram puts its actors and lifelines before its <style>:
    // anything inserted after the style would be drawn over them.
    fake.render.mockImplementationOnce(async (id: string) => ({
      svg: `<svg id="${id}" width="300" height="200" viewBox="0 0 300 200">`
        + '<g class="actor-top"><rect width="10" height="10"/></g><g class="actor-top"/><g class="actor-line"/>'
        + '<g class="actor-line"/><g class="actor-bottom"/><g class="actor-bottom"/>'
        + `<style>#${id} .actor{fill:#2d2d2d;}</style><g class="messages"><line x2="10"/></g></svg>`,
    }));
    const mounted = await mountMermaid(stage, 'sequenceDiagram\n  A->>B: hi', { theme: 'dark', width: 680 });
    const drawn = [...stage.querySelector('svg')!.children].map(child => `${child.localName}.${child.getAttribute('class') ?? ''}`);
    const root = new DOMParser().parseFromString((await mounted.toSvg!())!, 'image/svg+xml').documentElement;
    expect([...root.children].map(child => `${child.localName}.${child.getAttribute('class') ?? ''}`)).toEqual(drawn);
    expect(drawn).toHaveLength(8);
    expect(root.querySelectorAll(':scope > rect')).toHaveLength(0);
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

  it('reports a syntax error in one short line, with its reason, and draws nothing', async () => {
    // mermaid's own text: where, the code, a caret line, and why.
    fake.parse.mockRejectedValueOnce(new Error(
      "Parse error on line 2:\n...lowchart LR  A --> ;\n----------------------^\n"
      + "Expecting 'AMP', 'COLON', 'PIPE', 'TESTSTR', 'DOWN', 'DEFAULT', 'NUM', 'COMMA', 'NODE_STRING', got 'SEMI'",
    ));
    await expect(mountMermaid(stage, 'flowchart LR\n  A --> ;', { theme: 'dark', width: 680 }))
      .rejects.toThrow(/^Parse error on line 2: \.\.\.lowchart LR {2}A --> ; Expecting 'AMP', 'COLON', 'PIPE', 'TESTSTR', …; got 'SEMI'$/);
    expect(fake.render).not.toHaveBeenCalled();

    fake.parse.mockRejectedValueOnce(new Error(`Parse error on line 9:\n${'x'.repeat(400)}\n---^\nExpecting 'A', got 'B'`));
    const long = await mountMermaid(stage, 'flowchart LR\n  A', { theme: 'dark', width: 680 }).then(() => '', (err: Error) => err.message);
    expect(long.length).toBeLessThanOrEqual(240);
    expect(long.endsWith('…')).toBe(true);
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

  describe('once the picture is laid out', () => {
    // jsdom lays nothing out. In its place: boxes read from data- attributes
    // (data-box="x,y,width,height" on screen, data-bbox for the drawing's
    // content), and computed styles from data-fill / data-font.
    const numbers = (value: string | null) => (value ?? '').split(',').filter(Boolean).map(Number);
    beforeEach(() => {
      vi.spyOn(Element.prototype, 'getBoundingClientRect').mockImplementation(function (this: Element) {
        const [x = 0, y = 0, width = 0, height = 0] = numbers(this.getAttribute('data-box'));
        // The picture itself reports a size where it is laid out: anywhere but
        // in a stage marked as hidden (the frame's source view is showing).
        const hidden = !!this.closest('[data-hidden-stage]');
        const [w, h] = this.localName === 'svg' && !width ? (hidden ? [0, 0] : [400, 200]) : [width, height];
        return { x, y, left: x, top: y, width: w, height: h, right: x + w, bottom: y + h, toJSON: () => ({}) } as DOMRect;
      });
      vi.spyOn(window, 'getComputedStyle').mockImplementation(element => ({
        fill: element.getAttribute('data-fill') ?? '',
        fontSize: element.getAttribute('data-font') ?? '14px',
        display: (element as HTMLElement).style?.display ?? '',
      }) as CSSStyleDeclaration);
      Object.defineProperty(SVGElement.prototype, 'getBBox', {
        configurable: true,
        value(this: SVGElement) {
          const [x = 0, y = 0, width = 0, height = 0] = numbers(this.getAttribute('data-bbox'));
          return { x, y, width, height };
        },
      });
    });
    afterEach(() => {
      vi.restoreAllMocks();
      delete (SVGElement.prototype as { getBBox?: unknown }).getBBox;
    });

    const render = (svg: string) => fake.render.mockImplementationOnce(async (id: string) => ({ svg: svg.replace('ID', id) }));

    it('trims the bounds to the drawing with an even margin, and the size and the file follow', async () => {
      // mermaid leaves a sequence diagram or a pie far more room below than above.
      render('<svg id="ID" width="400" height="200" viewBox="0 0 400 200" data-bbox="50,20,200,100"><g/></svg>');
      const mounted = await mountMermaid(stage, 'pie\n  "a": 1', { theme: 'dark', width: 680 });
      expect(mounted.naturalSize).toEqual({ width: 216, height: 116 });
      const svg = stage.querySelector('svg')!;
      expect(svg.getAttribute('viewBox')).toBe('42 12 216 116');
      expect([svg.getAttribute('width'), svg.getAttribute('height')]).toEqual(['216', '116']);
      const file = new DOMParser().parseFromString((await mounted.toSvg!())!, 'image/svg+xml').documentElement;
      expect([file.getAttribute('viewBox'), file.getAttribute('width'), file.getAttribute('height')]).toEqual(['42 12 216 116', '216', '116']);
    });

    it('measures a copy out of sight when the stage is not laid out, and leaves nothing behind', async () => {
      stage.setAttribute('data-hidden-stage', '');
      const before = document.body.children.length;
      render('<svg id="ID" width="400" height="200" viewBox="0 0 400 200" data-bbox="50,20,200,100"><g/></svg>');
      const mounted = await mountMermaid(stage, 'pie\n  "a": 1', { theme: 'dark', width: 680 });
      expect(mounted.naturalSize).toEqual({ width: 216, height: 116 });
      expect(stage.querySelector('svg')!.getAttribute('viewBox')).toBe('42 12 216 116');
      expect(document.body.children.length).toBe(before);
      expect(document.querySelectorAll('svg[id^="nerve-mermaid-"]')).toHaveLength(1);
    });

    it('never grows the bounds: what lies outside mermaid\'s own stays as mermaid had it', async () => {
      render('<svg id="ID" width="400" height="200" viewBox="0 0 400 200" data-bbox="-20,-20,500,300"><g/></svg>');
      const mounted = await mountMermaid(stage, 'pie\n  "a": 1', { theme: 'dark', width: 680 });
      expect(mounted.naturalSize).toEqual({ width: 400, height: 200 });
      expect(stage.querySelector('svg')!.getAttribute('viewBox')).toBe('0 0 400 200');
    });

    it('puts an edge label on the colour of the subgraph under it', async () => {
      render('<svg id="ID" width="400" height="200" viewBox="0 0 400 200">'
        + '<g class="cluster"><rect data-box="0,0,300,200" data-fill="rgb(1, 2, 3)"/></g>'
        + '<g class="cluster"><rect data-box="100,50,100,100" data-fill="rgb(4, 5, 6)"/></g>'
        + '<g class="edgeLabel"><foreignObject><div><span class="edgeLabel"><p data-box="140,90,20,10">in both</p></span></div></foreignObject></g>'
        + '<g class="edgeLabel"><foreignObject><div><span class="edgeLabel"><p data-box="20,20,20,10">in one</p></span></div></foreignObject></g>'
        + '<g class="edgeLabel"><foreignObject><div><span class="edgeLabel"><p data-box="350,20,20,10">outside</p></span></div></foreignObject></g>'
        + '</svg>');
      await mountMermaid(stage, 'flowchart LR\n  A --> B', { theme: 'dark', width: 680 });
      const chips = [...stage.querySelectorAll<HTMLElement>('.edgeLabel p')];
      // The innermost subgraph decides; outside any, the theme's own chip stays.
      expect(chips.map(chip => chip.style.backgroundColor)).toEqual(['rgb(4, 5, 6)', 'rgb(1, 2, 3)', '']);
    });

    it('lifts text under 11px to 11px, and hides what a treemap shrank to fit a cell', async () => {
      render('<svg id="ID" width="400" height="200" viewBox="0 0 400 200">'
        + '<text data-font="9px">small</text><text data-font="14px">fine</text>'
        + '<text class="treemapLabel" data-font="6px">tiny</text></svg>');
      await mountMermaid(stage, 'treemap-beta\n"a": 1', { theme: 'dark', width: 680 });
      const [small, fine, tiny] = [...stage.querySelectorAll<SVGTextElement>('text')];
      expect(small.style.fontSize).toBe('11px');
      expect(fine.style.fontSize).toBe('');
      expect(tiny.style.display).toBe('none');
    });
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
