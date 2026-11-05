import DOMPurify from 'dompurify';
import type { Mermaid, MermaidConfig } from 'mermaid';
import { contrastRatio, luminance, mix, readThemeTokens, type ThemeTokens } from './tokens';
import type { Mount } from './types';

// mermaid is a few megabytes; it is fetched the first time a chat shows a
// diagram, and never for a chat that shows none.
let loading: Promise<Mermaid> | null = null;

function loadMermaid(): Promise<Mermaid> {
  loading ??= import('mermaid').then(module => module.default);
  return loading;
}

/**
 * mermaid's theme variables: hex colours, a few sizes and switches, and the
 * per-diagram blocks (`xyChart`, `radar`, …) some diagram types read.
 */
export type MermaidThemeVariables = Record<string, string | number | boolean | Record<string, string>>;

export interface MermaidTheme {
  /** For mermaid's `base` theme, the one built to take variables. */
  themeVariables: MermaidThemeVariables;
  /** Shape details no variable reaches. mermaid scopes it to the diagram's own id. */
  themeCSS: string;
}

/** WCAG AA for body text. */
const READABLE = 4.5;

/** Where the hues with a meaning sit in `tokens.palette` (the order of `PALETTE_VARS` in tokens.ts). */
const HUE = { blue: 0, orange: 1, green: 3, yellow: 5, violet: 6, teal: 7, red: 8 } as const;

/** How many colours mermaid cycles through for sections and series (its `THEME_COLOR_LIMIT`). */
const COLOR_SLOTS = 12;

/**
 * `color` washed into `paper` by `amount` (0–1), backing off until `text`
 * still reads on the result: a tinted surface for labels to sit on.
 */
function wash(paper: string, color: string, text: string, amount: number): string {
  for (let step = Math.round(amount * 100); step > 0; step -= 2) {
    const tint = mix(paper, color, step / 100);
    if (contrastRatio(tint, text) >= READABLE) return tint;
  }
  return paper;
}

/**
 * `from` moved towards `to`, by `amount` or as much further as it takes to
 * stand `minimum`:1 apart from `against` — a border that reads as an edge
 * (WCAG asks 3:1 of the shapes a picture is understood by).
 */
function towards(from: string, to: string, amount: number, against: string, minimum: number): string {
  for (let step = Math.round(amount * 100); step < 100; step += 2) {
    const color = mix(from, to, step / 100);
    if (contrastRatio(color, against) >= minimum) return color;
  }
  return to;
}

/**
 * The text colour for labels on `fills` (one colour for all of them, so the
 * worst fill decides): the theme's lightest or darkest neutral when one of
 * them reads on every fill, else plain white or black, whichever does better.
 */
function inkFor(fills: string[], t: ThemeTokens): string {
  const worst = (ink: string) => Math.min(...fills.map(fill => contrastRatio(ink, fill)));
  const byLuminance = [t.text, t.bg, t.surface].sort((a, b) => luminance(a) - luminance(b));
  const themed = [byLuminance[byLuminance.length - 1], byLuminance[0]].sort((a, b) => worst(b) - worst(a))[0];
  if (worst(themed) >= READABLE) return themed;
  return worst('#ffffff') >= worst('#000000') ? '#ffffff' : '#000000';
}

/** The colours every part of the theme is drawn from, by role. */
interface Roles {
  t: ThemeTokens;
  /** What the picture is drawn on: the figure's surface. */
  paper: string;
  text: string;
  muted: string;
  node: string;
  nodeBorder: string;
  /** Edges, arrows, axes. */
  line: string;
  /** Hairlines that should recede: grids, lifelines, guides. */
  guide: string;
  clusterFill: string;
  clusterBorder: string;
  palette: string[];
  hue(i: number): string;
  /** A hue as a fill that text sits on. */
  tint(color: string, amount?: number): string;
  /** The outline of a tinted fill. */
  rim(color: string): string;
  /** Text on a solid fill. */
  inkOn(fill: string): string;
}

function rolesFor(t: ThemeTokens): Roles {
  const paper = t.surface;
  const palette = t.palette.length > 0 ? t.palette : [t.accent];
  return {
    t,
    paper,
    text: t.text,
    muted: t.textMuted,
    node: t.surfaceRaised,
    nodeBorder: towards(t.border, t.text, 0.35, t.surfaceRaised, 3),
    line: t.textMuted,
    guide: mix(t.border, t.text, 0.18),
    clusterFill: mix(t.surface, t.accent, 0.06),
    clusterBorder: mix(t.border, t.accent, 0.4),
    palette,
    hue: i => palette[i % palette.length],
    tint: (color, amount = 0.24) => wash(paper, color, t.text, amount),
    rim: color => mix(paper, color, 0.7),
    inkOn: fill => inkFor([fill], t),
  };
}

/**
 * The app's palette as mermaid theme variables, for every diagram type
 * mermaid's `base` theme knows: neutral nodes with a visible border on the
 * figure's surface, lines in the muted text colour, the categorical palette
 * for series, slices and branches, and tinted fills wherever text sits on a
 * colour. Everything the base theme would otherwise derive from its own
 * pastel `primaryColor` is stated, so no stock grey, blue or cream is left
 * over. Pure: tokens in, variables out.
 */
export function mermaidThemeFor(tokens: ThemeTokens): MermaidTheme {
  const r = rolesFor(tokens);
  const { t, paper, text, muted, node, nodeBorder, line, guide, clusterFill, clusterBorder, hue, tint, rim, inkOn } = r;
  const blue = hue(HUE.blue);
  const red = hue(HUE.red);
  const yellow = hue(HUE.yellow);
  const violet = hue(HUE.violet);
  const green = hue(HUE.green);
  const orange = hue(HUE.orange);
  const seriesInk = inkFor(r.palette.slice(0, COLOR_SLOTS), t);

  const vars: MermaidThemeVariables = {
    darkMode: t.theme === 'dark',
    background: t.bg,
    fontFamily: t.fontFamily,
    fontSize: '14px',
    // The base theme strokes "neo" shapes with a gradient and hangs a grey
    // drop shadow under them; a flat figure has neither.
    useGradient: false,
    dropShadow: 'none',
    strokeWidth: 1.25,
    // Corner radius of the explicitly rounded shapes; plain boxes get 6px in themeCSS.
    radius: 12,

    // The ramp the base theme derives everything else from.
    primaryColor: node,
    primaryTextColor: text,
    primaryBorderColor: nodeBorder,
    secondaryColor: clusterFill,
    secondaryTextColor: text,
    secondaryBorderColor: clusterBorder,
    tertiaryColor: paper,
    tertiaryTextColor: text,
    tertiaryBorderColor: nodeBorder,

    // Flowchart, and the node/edge/cluster vocabulary most diagrams share.
    mainBkg: node,
    nodeBkg: node,
    nodeBorder,
    nodeTextColor: text,
    textColor: text,
    titleColor: text,
    lineColor: line,
    arrowheadColor: line,
    defaultLinkColor: line,
    edgeLabelBackground: paper,
    clusterBkg: clusterFill,
    clusterBorder,
    flowContainerStroke: clusterBorder,
    border2: nodeBorder,
    noteBkgColor: tint(yellow, 0.16),
    noteBorderColor: rim(yellow),
    noteTextColor: text,
    errorBkgColor: tint(red),
    errorTextColor: text,

    // Sequence.
    actorBkg: node,
    actorBorder: nodeBorder,
    actorTextColor: text,
    actorLineColor: guide,
    signalColor: line,
    signalTextColor: text,
    labelBoxBkgColor: node,
    labelBoxBorderColor: nodeBorder,
    labelTextColor: text,
    loopTextColor: text,
    activationBkgColor: mix(node, text, 0.08),
    activationBorderColor: nodeBorder,
    sequenceNumberColor: inkOn(line),
    rectBkgColor: mix(paper, text, 0.05),

    // State.
    stateBkg: node,
    stateBorder: nodeBorder,
    stateLabelColor: text,
    transitionColor: line,
    transitionLabelColor: text,
    labelBackgroundColor: paper,
    compositeBackground: clusterFill,
    compositeTitleBackground: clusterFill,
    compositeBorder: nodeBorder,
    altBackground: mix(paper, text, 0.04),
    specialStateColor: line,
    innerEndBackground: line,

    // Class, ER, requirement, C4, use case.
    classText: text,
    attributeBackgroundColorOdd: node,
    attributeBackgroundColorEven: mix(node, text, 0.04),
    rowOdd: node,
    rowEven: mix(node, text, 0.04),
    requirementBackground: node,
    requirementBorderColor: nodeBorder,
    requirementTextColor: text,
    relationColor: line,
    relationLabelBackground: paper,
    relationLabelColor: text,
    personBkg: node,
    personBorder: nodeBorder,

    // Gantt: bars are tinted so their labels read; each section takes its
    // own hue in themeCSS, done is grey, critical is red.
    sectionBkgColor: t.textFaint,
    sectionBkgColor2: t.textFaint,
    altSectionBkgColor: paper,
    excludeBkgColor: mix(paper, text, 0.06),
    gridColor: guide,
    taskBkgColor: tint(blue, 0.3),
    taskBorderColor: rim(blue),
    taskTextColor: text,
    taskTextLightColor: text,
    taskTextDarkColor: text,
    taskTextOutsideColor: text,
    taskTextClickableColor: text,
    activeTaskBkgColor: tint(blue, 0.5),
    activeTaskBorderColor: blue,
    doneTaskBkgColor: mix(paper, muted, 0.16),
    doneTaskBorderColor: mix(t.border, text, 0.25),
    critBkgColor: tint(red, 0.3),
    critBorderColor: red,
    todayLineColor: red,
    vertLineColor: muted,

    // Pie: solid slices parted by the paper, one ink for every slice label.
    pieTitleTextColor: text,
    pieTitleTextSize: '16px',
    pieSectionTextColor: seriesInk,
    pieSectionTextSize: '13px',
    pieLegendTextColor: text,
    pieLegendTextSize: '14px',
    pieStrokeColor: paper,
    pieStrokeWidth: '2px',
    pieOuterStrokeColor: paper,
    pieOuterStrokeWidth: '0px',
    pieOpacity: '1',

    // gitGraph: a hue per branch, with branch names in whichever ink reads on it.
    branchLabelColor: inkOn(blue),
    commitLabelColor: muted,
    commitLabelBackground: paper,
    commitLabelFontSize: '11px',
    commitLineColor: guide,
    tagLabelColor: text,
    tagLabelBackground: node,
    tagLabelBorder: nodeBorder,
    tagLabelFontSize: '11px',

    // Quadrant chart: one hue, strongest in the first quadrant.
    quadrant1Fill: tint(blue, 0.2),
    quadrant2Fill: tint(blue, 0.11),
    quadrant3Fill: tint(blue, 0.04),
    quadrant4Fill: tint(blue, 0.11),
    quadrant1TextFill: text,
    quadrant2TextFill: text,
    quadrant3TextFill: text,
    quadrant4TextFill: text,
    quadrantPointFill: blue,
    quadrantPointTextFill: text,
    quadrantXAxisTextFill: text,
    quadrantYAxisTextFill: text,
    quadrantInternalBorderStrokeFill: paper,
    quadrantExternalBorderStrokeFill: nodeBorder,
    quadrantTitleFill: text,

    // Timeline, mindmap, kanban, treemap: their sections take cScale (below).
    scaleLabelColor: text,

    // Journey: the smiley stays yellow.
    faceColor: mix(paper, yellow, t.theme === 'dark' ? 0.75 : 0.35),

    // Venn, architecture, event modeling, Wardley.
    vennTitleTextColor: text,
    vennSetTextColor: text,
    archEdgeColor: line,
    archEdgeArrowColor: line,
    archGroupBorderColor: clusterBorder,
    emUiFill: node,
    emUiStroke: nodeBorder,
    emProcessorFill: tint(violet),
    emProcessorStroke: rim(violet),
    emReadModelFill: tint(green),
    emReadModelStroke: rim(green),
    emCommandFill: tint(blue),
    emCommandStroke: rim(blue),
    emEventFill: tint(orange),
    emEventStroke: rim(orange),
    emSwimlaneBackgroundOdd: mix(paper, text, 0.03),
    emSwimlaneBackgroundStroke: t.border,
    emArrowhead: line,
    emRelationStroke: line,
    wardleyEvolutionColor: red,

    xyChart: {
      backgroundColor: paper,
      titleColor: text,
      dataLabelColor: seriesInk,
      legendTextColor: text,
      xAxisTitleColor: text,
      xAxisLabelColor: muted,
      xAxisTickColor: nodeBorder,
      xAxisLineColor: nodeBorder,
      yAxisTitleColor: text,
      yAxisLabelColor: muted,
      yAxisTickColor: nodeBorder,
      yAxisLineColor: nodeBorder,
      plotColorPalette: r.palette.join(','),
    },
    radar: {
      axisColor: line,
      graticuleColor: guide,
    },
    cynefin: {
      boundaryColor: line,
      cliffColor: red,
      arrowColor: line,
      complexBg: tint(green, 0.14),
      complicatedBg: tint(blue, 0.14),
      chaoticBg: tint(red, 0.14),
      clearBg: tint(yellow, 0.14),
      confusionBg: tint(violet, 0.14),
      textColor: text,
      labelColor: text,
    },
    wardley: {
      backgroundColor: paper,
      axisColor: line,
      axisTextColor: text,
      gridColor: guide,
      componentFill: paper,
      componentStroke: line,
      componentLabelColor: text,
      linkStroke: line,
      evolutionStroke: red,
      annotationStroke: line,
      annotationTextColor: text,
      annotationFill: paper,
    },
    packet: {
      byteFontSize: '11px',
      startByteColor: muted,
      endByteColor: muted,
      labelColor: text,
      titleColor: text,
      blockStrokeColor: nodeBorder,
      blockFillColor: node,
    },
    treeView: {
      labelColor: text,
      lineColor: line,
      iconColor: muted,
      descriptionColor: muted,
      highlightBg: tint(yellow, 0.2),
      highlightStroke: rim(yellow),
    },
  };

  for (let i = 0; i < COLOR_SLOTS; i++) {
    vars[`pie${i + 1}`] = hue(i);
    // Section fills of timeline, mindmap, kanban and treemap, with text on them.
    vars[`cScale${i}`] = tint(hue(i), 0.28);
    vars[`cScalePeer${i}`] = rim(hue(i));
    vars[`cScaleInv${i}`] = hue(i);
    vars[`cScaleLabel${i}`] = text;
  }
  for (let i = 0; i < 8; i++) {
    vars[`git${i}`] = hue(i);
    // The ring around a highlighted commit: it has to stand out from the paper, not the branch.
    vars[`gitInv${i}`] = text;
    vars[`gitBranchLabel${i}`] = inkOn(hue(i));
    vars[`fillType${i}`] = tint(hue(i));
    vars[`venn${i + 1}`] = hue(i);
  }
  for (let i = 0; i < 6; i++) vars[`actor${i}`] = hue(i);

  return { themeVariables: vars, themeCSS: themeCssFor(r) };
}

/**
 * What the variables cannot say. Selectors are checked against mermaid's
 * output; mermaid prefixes each rule with the diagram's `#id`, which keeps
 * them off the rest of the page and on the picture when it is saved alone.
 */
function themeCssFor({ paper, muted, line, guide, clusterFill, hue, tint, rim }: Roles): string {
  const rules = [
    // "neo" shapes carry a drop-shadow filter as an attribute.
    '[filter*="drop-shadow"] { filter: none; }',
    // Plain boxes get the app's corner radius; shapes drawn with a radius of
    // their own (rounded, stadium, state) keep theirs.
    '.node rect.label-container:not([rx]), .node rect.basic:not([rx]) { rx: 6px; ry: 6px; }',
    '.cluster rect { rx: 8px; ry: 8px; }',
    '.node rect, .node polygon, .node circle, .node ellipse, .node path { stroke-width: 1.25px; }',
    // Edge labels: a small chip that cuts the line, the same in every diagram type.
    '.labelBkg { background-color: transparent; }',
    `.edgeLabel p { background-color: ${paper}; padding: 1px 5px; border-radius: 4px; }`,
    // SVG-text labels: the same chip, its padding drawn as a stroke in the chip's colour.
    `.edgeLabel rect.background, .edgeLabel .label rect { opacity: 1; fill: ${paper}; stroke: ${paper}; stroke-width: 6px; stroke-linejoin: round; rx: 3px; ry: 3px; }`,
    // Titles in one size; the timeline's has no class, only its 4ex size.
    '.flowchartTitleText, .statediagramTitleText, .classTitleText, .gitTitleText, .titleText, .pieTitleText, .venn-title, text[font-size="4ex"] { font-size: 16px; font-weight: 600; }',
    // Sequence lifelines are hairlines.
    `.actor-line { stroke-width: 1px; }`,
    // Journey and timeline draw their axes and arrowheads in black (or the
    // text colour); they are lines like any other.
    `line[marker-end][stroke-width="4"] { stroke: ${line}; }`,
    `line[marker-end][stroke-dasharray] { stroke: ${guide}; }`,
    `.task-line { stroke: ${guide}; }`,
    `[id$="-arrowhead"] path { fill: ${line}; stroke: none; }`,
    // C4 draws relations and boundaries in a fixed #444444, unreadable on a dark figure.
    `[stroke="#444444"] { stroke: ${line}; }`,
    `text[fill="#444444"] { fill: ${muted}; }`,
    // Sequence half-arrows (-\\ -//) and async heads: black strokes or the
    // text colour by default; lines like the rest.
    `[id$="-stickTopArrowHead"] path, [id$="-stickBottomArrowHead"] path { stroke: ${line}; }`,
    `[id$="-solidTopArrowHead"] path, [id$="-solidBottomArrowHead"] path, [id$="-filled-head"] path { fill: ${line}; stroke: none; }`,
    // A composite state is one tinted container, like a subgraph: the title
    // on the tint, and no outline round the body (whose bottom stops 4px
    // short of the container's and showed as a lip).
    `[data-look="neo"].statediagram-cluster rect.outer, .statediagram-cluster rect.outer { fill: ${clusterFill}; }`,
    '.statediagram-cluster rect.inner { stroke: none; }',
    // The xychart's title, weighted like every other title.
    '.chart-title text { font-weight: 600; }',
    // Text floor: 12px for axis labels (d3's axes say 10), 11px for anything else.
    '.grid .tick text { font-size: 12px; }',
    '.c4-shape .label .c4-type { font-size: 11px; }',
    // A treemap writes its section totals as an inline 10px.
    '.treemapSectionValue { font-size: 11px !important; }',
  ];
  // Gantt: each section's bars in their own hue (mermaid styles four).
  for (let i = 0; i < 4; i++) {
    rules.push(`.task${i} { fill: ${tint(hue(i), 0.3)}; stroke: ${rim(hue(i))}; }`);
    rules.push(`.active${i} { fill: ${tint(hue(i), 0.5)}; stroke: ${hue(i)}; }`);
  }
  // Radar curves are data: the plain hue, not a section tint.
  for (let i = 0; i < COLOR_SLOTS; i++) {
    rules.push(`.radarCurve-${i}, .radarLegendBox-${i} { color: ${hue(i)}; fill: ${hue(i)}; stroke: ${hue(i)}; }`);
  }
  return rules.join('\n');
}

/** The diagram types with a `useMaxWidth`: all are drawn at their natural size; the frame fits or scrolls. */
const DIAGRAM_KEYS = [
  'flowchart', 'swimlane', 'agentflow', 'sequence', 'gantt', 'journey', 'timeline', 'class', 'state', 'er',
  'pie', 'quadrantChart', 'xyChart', 'requirement', 'architecture', 'mindmap', 'ishikawa', 'kanban',
  'gitGraph', 'c4', 'sankey', 'packet', 'block', 'eventmodeling', 'treeView', 'radar', 'usecase', 'venn',
  'wardley-beta', 'cynefin', 'railroad', 'treemap',
] as const;

/** Everything mermaid is told before a render; `width` is the column a gantt chart or an xychart spans. */
function configFor(tokens: ThemeTokens, width: number): MermaidConfig {
  const { themeVariables, themeCSS } = mermaidThemeFor(tokens);
  const font = tokens.fontFamily;
  const diagrams: Record<string, Record<string, unknown>> = {};
  for (const key of DIAGRAM_KEYS) diagrams[key] = { useMaxWidth: false };
  Object.assign(diagrams.sequence, {
    actorFontFamily: font, noteFontFamily: font, messageFontFamily: font,
    actorFontSize: 14, noteFontSize: 13, messageFontSize: 14,
  });
  Object.assign(diagrams.journey, { taskFontFamily: font, titleFontFamily: font, titleFontSize: '16px' });
  Object.assign(diagrams.timeline, { taskFontFamily: font });
  // A gantt chart is as wide as the element it is drawn in, which during a
  // render is a scratch element on the page body; it spans the column instead.
  // Its axis labels are 12px; mermaid's default date format (2026-01-05) is
  // too long for that at d3's ten-odd ticks a column, "Jan 5" is not. A
  // diagram's own `axisFormat` still wins.
  Object.assign(diagrams.gantt, {
    fontSize: 12, sectionFontSize: 12, useWidth: width > 0 ? Math.round(width) : 720, axisFormat: '%b %-d',
  });
  // An xychart, like a Vega chart, spans the column (it would otherwise be
  // drawn at 700px and shrunk, small text and all, into a narrower one): a
  // 16px title, 12px axis labels, hairline axes.
  const chartWidth = width > 0 ? Math.round(width) : 700;
  const axis = { labelFontSize: 12, titleFontSize: 13, tickWidth: 1, axisLineWidth: 1 };
  Object.assign(diagrams.xyChart, {
    width: chartWidth,
    height: Math.round(Math.min(400, Math.max(240, chartWidth * 0.57))),
    titleFontSize: 16,
    legendFontSize: 12,
    xAxis: axis,
    yAxis: { ...axis },
  });
  // Radar axis labels sit just outside the circle and clip at mermaid's 50px margins.
  Object.assign(diagrams.radar, { marginLeft: 90, marginRight: 90 });
  Object.assign(diagrams.quadrantChart, {
    titleFontSize: 16, xAxisLabelFontSize: 13, yAxisLabelFontSize: 13, quadrantLabelFontSize: 14, pointLabelFontSize: 12,
  });
  return {
    startOnLoad: false,
    // Labels are sanitized, and `click` callbacks and links in the diagram
    // are ignored: the diagram is drawn, never run.
    securityLevel: 'strict',
    // mermaid puts label HTML into the page while it measures it, so what
    // could load must be gone before that, not after the render.
    dompurifyConfig: LABEL_PURIFY,
    secure: SECURE_KEYS,
    // A failed render must not leave mermaid's error picture on the page body.
    suppressErrorRendering: true,
    theme: 'base',
    darkMode: tokens.theme === 'dark',
    fontFamily: font,
    fontSize: 14,
    themeVariables,
    themeCSS,
    ...diagrams,
  } as MermaidConfig;
}

// What follows keeps the promise every renderer makes (types.ts): load
// nothing from the network. mermaid's `strict` level stops scripts, not
// fetches: label HTML may carry an <img>, an inline style with a url(), an
// inline <svg><image>; an image node, or a sequence actor's icon, is fetched
// by mermaid itself while it draws; a diagram's own front matter or
// %%{init}%% directive may set themeCSS, or a fontFamily that escapes into
// the stylesheet. The source is model output, and model output can carry a
// URL with something from a tool result in its query string.

/**
 * Label HTML keeps text formatting and links; nothing that loads or embeds.
 * mermaid sanitizes its own icons (architecture services, icon shapes) with
 * the same settings, and they are inline <svg> coloured by `style`
 * attributes: those stay, and the hook below takes any attribute, a style
 * among them, that reaches out of the picture.
 */
const LABEL_PURIFY = {
  FORBID_TAGS: [
    'style', 'img', 'image', 'feImage', 'use', 'foreignObject', 'math', 'picture', 'source', 'video', 'audio',
    'track', 'iframe', 'frame', 'object', 'embed', 'link', 'meta', 'base', 'form', 'input',
  ],
  FORBID_ATTR: ['src', 'srcset', 'background', 'poster'],
};

/** Keys a diagram's front matter or %%{init}%% directive may not set: mermaid's own, and those that reach the stylesheet. */
const SECURE_KEYS = [
  'secure', 'securityLevel', 'startOnLoad', 'maxTextSize', 'suppressErrorRendering', 'maxEdges',
  'themeCSS', 'fontFamily', 'altFontFamily',
];

/** A reference that stays inside the picture, or a picture inline: `#id`, `data:image/…`. */
const LOCAL_REF = /^\s*(#|data:image\/)/i;

function valuesOf(collection: unknown): unknown[] {
  if (collection instanceof Map) return [...collection.values()];
  return collection && typeof collection === 'object' ? Object.values(collection) : [];
}

/**
 * The URL mermaid would fetch while drawing this diagram, if any: an image
 * node's `img`, or a sequence actor's `icon` (`@name` is a built-in symbol).
 * Read from the parsed diagram rather than the source, so no quoting or
 * escape in the source can hide one.
 */
function fetchedWhileDrawing(db: unknown): string | undefined {
  const parsed = db as { getVertices?: () => unknown; getActors?: () => unknown };
  const urls = [
    ...valuesOf(parsed.getVertices?.()).map(vertex => (vertex as { img?: unknown } | null)?.img),
    ...valuesOf(parsed.getActors?.()).map(actor => (actor as { properties?: { icon?: unknown } } | null)?.properties?.icon),
  ];
  const external = urls.find(url => typeof url === 'string' && url.trim() !== '' && !/^\s*@/.test(url) && !LOCAL_REF.test(url));
  return external as string | undefined;
}

/**
 * Lines whose text becomes CSS (class and style definitions, C4 style
 * updates, sequence boxes), live on the page while mermaid draws.
 */
const STYLE_LINE = /^\s*(?:classDef|style|linkStyle|class|cssClass|Update\w*Style|rect|box)\b/i;

/**
 * Why `source` could make the page fetch while mermaid draws it, if it
 * could: a `url()` out of the picture anywhere, or on a style line an
 * escape (which can spell one), an import or an image set. A backslash in a
 * label (a Windows path) is only text.
 */
function reachesOut(source: string): string | undefined {
  const url = /url\(\s*['"]?\s*(?!#)[^)\n]{0,80}/i.exec(source);
  if (url) return `its styles point outside the picture (${url[0]})`;
  const line = source.split('\n').find(text => STYLE_LINE.test(text) && /\\|@import\b|image-set\(|\bsrc\(/i.test(text));
  return line ? `a style in it is escaped or imports (${line.trim().slice(0, 80)})` : undefined;
}

/** CSS that could name something to fetch: a reference out of the picture, or an escape that could spell one. */
const EXTERNAL_CSS = /\\|url\(\s*['"]?\s*(?!#)|@import\b|image-set\(|\bsrc\(/i;

/** CSS with every reference out of the picture made inert: `url(…)` to `none`, imports and image sets broken. */
function inertCss(css: string): string {
  return css
    // An escape spells `url(` without its letters (`\75 rl(`). mermaid's own
    // CSS has none, so every backslash goes and what is left says what it means.
    .replace(/\\/g, '')
    .replace(/url\(\s*("[^"]*"|'[^']*'|[^)]*)\s*\)?/gi, (whole, target: string) => (/^["']?\s*#/.test(target) ? whole : 'none'))
    .replace(/@import\b/gi, '@-nerve-blocked')
    .replace(/\b(image-set|src)\(/gi, '-nerve-blocked-$1(');
}

let sanitizerGuarded = false;

/**
 * Attributes that reach out of the picture (`fill="url(https://…)"`, an
 * escape that spells one) go wherever mermaid sanitizes label HTML. The hook
 * is on DOMPurify's default instance, the one mermaid uses: nothing else in
 * the app does (svg.ts keeps an instance of its own), and no attribute of
 * mermaid's own output looks like this.
 */
function guardSanitizer(): void {
  if (sanitizerGuarded) return;
  sanitizerGuarded = true;
  DOMPurify.addHook('uponSanitizeAttribute', (_node, data) => {
    if (EXTERNAL_CSS.test(data.attrValue)) data.keepAttr = false;
  });
}

/** Elements that fetch or embed by nature; no diagram is made of them. */
const FETCHING_TAGS = new Set([
  'img', 'picture', 'source', 'video', 'audio', 'track', 'iframe', 'frame', 'object', 'embed',
  'link', 'meta', 'base', 'script', 'input',
]);

/** Attributes that name something to fetch. */
const URL_ATTRIBUTES = new Set(['src', 'srcset', 'poster', 'background', 'lowsrc', 'dynsrc', 'data', 'action', 'formaction']);

/**
 * mermaid's SVG, parsed where nothing loads (a template's content is inert),
 * with everything that would fetch once on the page taken out: fetching
 * elements, URL attributes, references that leave the picture (a link's
 * `href` stays: following it takes a click), attributes that reach out (a
 * style, a fill), and external `url()`s in the stylesheet.
 * The last line of defence; the measures above keep them out of the render.
 */
function inertPicture(svg: string): DocumentFragment {
  const template = document.createElement('template');
  template.innerHTML = svg;
  for (const element of [...template.content.querySelectorAll('*')]) {
    const tag = element.localName.toLowerCase();
    if (FETCHING_TAGS.has(tag)) {
      element.remove();
      continue;
    }
    for (const attribute of [...element.attributes]) {
      const name = attribute.localName.toLowerCase();
      if (
        URL_ATTRIBUTES.has(name)
        || (name === 'href' && tag !== 'a' && !LOCAL_REF.test(attribute.value))
        // No attribute of mermaid's own looks like this (a style, a fill);
        // a classDef's might, or an entity-encoded url() in label HTML.
        || EXTERNAL_CSS.test(attribute.value)
      ) {
        element.removeAttributeNode(attribute);
      }
    }
    if (tag === 'style' && EXTERNAL_CSS.test(element.textContent ?? '')) {
      element.textContent = inertCss(element.textContent ?? '');
    }
  }
  return template.content;
}

let renderSeq = 0;

/** How long a reason may be in the frame's one-line notice. */
const MESSAGE_LENGTH = 240;

/**
 * A mermaid parse error in one line: where it is, the code it quotes, and
 * why ("Expecting 'NODE_STRING', got ';'") — the caret line under the code
 * goes, and a long list of what was expected is cut to its first few.
 */
function describe(err: unknown): string {
  const message = err instanceof Error ? err.message : String(err);
  const lines = message.split('\n').map(line => line.trim()).filter(line => line && !/^-*\^$/.test(line));
  const why = lines.findIndex(line => /^Expecting\b/i.test(line));
  const head = lines.slice(0, why > 0 ? Math.min(why, 2) : 2);
  let reason = why >= 0 ? lines[why] : '';
  const expecting = /^Expecting (.+), got (.+)$/i.exec(reason);
  if (expecting) {
    const options = expecting[1].split(/,\s*/);
    reason = `Expecting ${options.slice(0, 4).join(', ')}${options.length > 4 ? ', …' : ''}; got ${expecting[2]}`;
  }
  const text = [...head, reason].filter(Boolean).join(' ');
  return (text.length > MESSAGE_LENGTH ? `${text.slice(0, MESSAGE_LENGTH - 1)}…` : text) || 'the diagram could not be parsed';
}

function viewBoxOf(svg: SVGSVGElement): { width: number; height: number } | undefined {
  const box = (svg.getAttribute('viewBox') ?? '').trim().split(/[\s,]+/).map(Number);
  return box.length === 4 && box[2] > 0 && box[3] > 0 ? { width: box[2], height: box[3] } : undefined;
}

/**
 * The picture's size in CSS pixels: its `width`, with the height its viewBox
 * implies (a journey diagram states a height that leaves its title out, and
 * would be drawn shrunk to fit it), else the viewBox alone.
 */
function naturalSizeOf(svg: SVGSVGElement): { width: number; height: number } | undefined {
  const w = svg.getAttribute('width') ?? '';
  const h = svg.getAttribute('height') ?? '';
  const width = w.endsWith('%') ? NaN : parseFloat(w);
  const height = h.endsWith('%') ? NaN : parseFloat(h);
  const box = viewBoxOf(svg);
  if (width > 0 && box) return { width, height: (width * box.height) / box.width };
  if (width > 0 && height > 0) return { width, height };
  return box;
}

/**
 * The SVG as a file of its own, at the size it is drawn at. The `<style>`
 * mermaid inlined travels with it, scoped by the root's own id. Nothing is
 * added: the background is the business of whoever shows or saves the file
 * (and mermaid puts some pictures' content before its `<style>`, where
 * anything inserted would cover it). XML serialization declares the SVG and
 * xlink namespaces a standalone document needs, and turns the labels' `<br>`
 * and `&nbsp;` into what a file can hold.
 */
function standalone(svg: SVGSVGElement, size: { width: number; height: number } | undefined): string {
  const copy = svg.cloneNode(true) as SVGSVGElement;
  // Sizing on the page (fit, zoom) is the page's business, not the file's.
  copy.removeAttribute('style');
  if (size) {
    copy.setAttribute('width', String(size.width));
    copy.setAttribute('height', String(size.height));
  }
  return new XMLSerializer().serializeToString(copy);
}

type Size = { width: number; height: number };

/** Room left round a drawing trimmed to its content, the same on every side. */
const MARGIN = 8;

/** The smallest text a picture may hold, in CSS pixels at its natural size. */
const SMALLEST_TEXT = 11;

/** What an edge label can sit on other than the figure: a subgraph, a composite state. */
const AREAS = '.cluster > rect, .statediagram-cluster rect.inner, .statediagram-cluster rect.outer';

/** The chip behind an edge label: its HTML paragraph, or the rect behind SVG text. */
const CHIPS = '.edgeLabel p, .edgeLabel rect.background';

/** Text a treemap shrank to fit its cell: below the floor it goes, rather than overflowing the cell. */
const SHRUNK_TO_FIT = '.treemapLabel, .treemapValue';

const round2 = (n: number) => Math.round(n * 100) / 100;

/** For each edge-label chip that sits on a subgraph or composite state, that area's fill. */
function chipFills(svg: SVGSVGElement): Map<number, string> {
  const areas = [...svg.querySelectorAll(AREAS)]
    .map(area => ({ box: area.getBoundingClientRect(), fill: getComputedStyle(area).fill }))
    .filter(area => area.box.width > 0 && area.box.height > 0 && /^rgba?\(/.test(area.fill));
  const fills = new Map<number, string>();
  if (areas.length === 0) return fills;
  [...svg.querySelectorAll(CHIPS)].forEach((chip, index) => {
    const box = chip.getBoundingClientRect();
    const x = box.left + box.width / 2;
    const y = box.top + box.height / 2;
    // The innermost area under the chip's centre: nested subgraphs, a state in a state.
    const under = areas
      .filter(area => x >= area.box.left && x <= area.box.right && y >= area.box.top && y <= area.box.bottom)
      .sort((a, b) => a.box.width * a.box.height - b.box.width * b.box.height)[0];
    if (under) fills.set(index, under.fill);
  });
  return fills;
}

/** Text under the floor, by index among the picture's `text` and `tspan` elements. */
function smallText(svg: SVGSVGElement, scale: number): Map<number, 'hide' | 'grow'> {
  const found = new Map<number, 'hide' | 'grow'>();
  [...svg.querySelectorAll('text, tspan')].forEach((element, index) => {
    const style = getComputedStyle(element);
    if (style.display === 'none' || !element.textContent?.trim()) return;
    if (parseFloat(style.fontSize) * scale < SMALLEST_TEXT) found.set(index, element.closest(SHRUNK_TO_FIT) ? 'hide' : 'grow');
  });
  return found;
}

/**
 * What only the laid-out picture can tell, put right on `svg`: no text under
 * the 11px floor; edge-label chips in the colour of the subgraph or state
 * they sit on, not boxes of figure colour on its tint; and bounds trimmed to
 * the drawing with an even margin (mermaid leaves sequence diagrams and pies
 * twice the room at the bottom). Trimming only shrinks: what mermaid's own
 * bounds include stays, every label among it. Measured in place when the
 * stage is laid out — the frame draws into one that is hidden but laid out —
 * else on a copy out of sight; nothing changes where nothing can be measured.
 * Returns the picture's size afterwards.
 */
function finish(svg: SVGSVGElement, size: Size): Size {
  const box = (svg.getAttribute('viewBox') ?? '').trim().split(/[\s,]+/).map(Number);
  if (typeof svg.getBBox !== 'function' || box.length !== 4 || !box.every(Number.isFinite) || box[2] <= 0) return size;
  const [x0, y0, w0, h0] = box;
  const scale = size.width / w0;
  let measured = svg;
  let host: HTMLElement | null = null;
  if (!(svg.getBoundingClientRect().width > 0)) {
    host = document.createElement('div');
    host.style.cssText = 'position: fixed; left: -100000px; top: 0; visibility: hidden; pointer-events: none;';
    measured = svg.cloneNode(true) as SVGSVGElement;
    host.appendChild(measured);
    document.body.appendChild(host);
  }
  const pairs = (selector: string) => {
    const real = [...svg.querySelectorAll<SVGElement | HTMLElement>(selector)];
    const copy = measured === svg ? real : [...measured.querySelectorAll<SVGElement | HTMLElement>(selector)];
    return { real, copy };
  };
  try {
    const small = smallText(measured, scale);
    if (small.size) {
      const text = pairs('text, tspan');
      for (const [index, action] of small) {
        for (const element of [text.real[index], text.copy[index]]) {
          if (!element) continue;
          if (action === 'hide') element.style.display = 'none';
          else element.style.fontSize = `${SMALLEST_TEXT / scale}px`;
        }
      }
    }
    const fills = chipFills(measured);
    if (fills.size) {
      const chips = pairs(CHIPS).real;
      for (const [index, fill] of fills) {
        const chip = chips[index];
        if (!chip) continue;
        if (chip.localName === 'p') chip.style.backgroundColor = fill;
        else Object.assign(chip.style, { fill, stroke: fill });
      }
    }
    const content = measured.getBBox();
    if (!(content.width > 0 && content.height > 0)) return size;
    const left = Math.max(x0, content.x - MARGIN);
    const top = Math.max(y0, content.y - MARGIN);
    const right = Math.min(x0 + w0, content.x + content.width + MARGIN);
    const bottom = Math.min(y0 + h0, content.y + content.height + MARGIN);
    if (right - left >= w0 - 0.5 && bottom - top >= h0 - 0.5) return size;
    svg.setAttribute('viewBox', [left, top, right - left, bottom - top].map(round2).join(' '));
    return { width: round2((right - left) * scale), height: round2((bottom - top) * scale) };
  } catch {
    // Measuring improves a picture; it never stops one from showing.
    return size;
  } finally {
    host?.remove();
  }
}

// `initialize` resets mermaid's one global configuration, so a render states
// all of it and runs alone: two diagrams in flight at once (a theme switch,
// two blocks settling together) must not draw with each other's settings.
let queue: Promise<unknown> = Promise.resolve();

function exclusive<T>(task: () => Promise<T>): Promise<T> {
  const run = queue.then(task, task);
  queue = run.catch(() => undefined);
  return run;
}

export const mountMermaid: Mount = async (stage, source, { theme, width }) => {
  // Read before anything is awaited, while the page still shows `theme`.
  const tokens = readThemeTokens(theme);
  const reason = reachesOut(source);
  if (reason) throw new Error(`${reason}, and pictures here load nothing from the network`);
  const mermaid = await loadMermaid();
  guardSanitizer();
  const svg = await exclusive(async () => {
    mermaid.initialize(configFor(tokens, width));
    let parsed: { db: unknown };
    try {
      // A syntax error is reported here, with the diagram's own message.
      await mermaid.parse(source);
      parsed = await mermaid.mermaidAPI.getDiagramFromText(source);
    } catch (err) {
      throw new Error(describe(err));
    }
    const url = fetchedWhileDrawing(parsed.db);
    if (url) {
      throw new Error(`it draws an image from ${url.slice(0, 80)}, and pictures here load nothing from the network`);
    }
    return (await mermaid.render(`nerve-mermaid-${++renderSeq}`, source)).svg;
  });
  stage.replaceChildren(inertPicture(svg));
  const element = stage.querySelector('svg');
  let naturalSize = element ? naturalSizeOf(element) : undefined;
  if (element && naturalSize) {
    // The picture states its own size (a few diagram types ask for
    // `width="100%"` whatever `useMaxWidth` says); the frame fits or scrolls it.
    const state = (size: Size) => {
      element.setAttribute('width', String(size.width));
      element.setAttribute('height', String(size.height));
    };
    state(naturalSize);
    element.style.removeProperty('max-width');
    naturalSize = finish(element, naturalSize);
    state(naturalSize);
  }
  // The file is made from the picture as drawn, whatever the frame does to
  // the element on screen afterwards.
  const drawn = element?.cloneNode(true) as SVGSVGElement | undefined;
  return {
    cleanup: () => {
      stage.innerHTML = '';
    },
    naturalSize,
    toSvg: async () => (drawn ? standalone(drawn, naturalSize) : null),
  };
};
