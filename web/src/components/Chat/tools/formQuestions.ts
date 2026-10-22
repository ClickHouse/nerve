/**
 * Nerve's `ask_form` tool without React: its questions, the person's answers,
 * and the result the server writes back. `FormBlock` renders it.
 *
 * The call is `ask_form(title?, questions)`, 1–6 questions of mixed kinds:
 * `choice` and `multi` pick from options (plus an Other… answer unless
 * `allow_other` is false), `text` and `url` are typed, `list` and `repos` are
 * lists of items. The web answers `{answers: {<id>: string | string[]}}`:
 * a string for choice/text/url, a list for multi/list/repos; an optional
 * question left empty is left out. On success the tool result is a fixed
 * header line, one `<label>: <answer>` line per question (`(skipped)` for none;
 * a multi-line answer continues on lines indented by two spaces), a blank
 * line, and the same answers as one line of JSON:
 * `{"answers": {...}, "skipped": [...]}`. An error result carries no answers.
 */

export type FormKind = 'choice' | 'multi' | 'text' | 'list' | 'repos' | 'url';

const KINDS: ReadonlySet<string> = new Set<FormKind>(['choice', 'multi', 'text', 'list', 'repos', 'url']);

export interface FormOption {
  label: string;
  description: string;
}

export interface FormQuestion {
  /** Unique in the call; the key of the answer. */
  id: string;
  kind: FormKind;
  /** The header chip ("Repositories"). */
  label: string;
  question: string;
  /** Why it is asked, shown muted under the question ('' for none). */
  help: string;
  placeholder: string;
  required: boolean;
  /** choice / multi only. */
  options: FormOption[];
  /** choice / multi: an Other… answer in the person's own words. */
  allowOther: boolean;
  /** The prefilled answer. */
  defaultValue: string | string[] | null;
}

export type FormAnswer = string | string[];
export type FormAnswers = Record<string, FormAnswer>;

/** The longest answer, or list item, the server keeps. */
export const MAX_ANSWER_LENGTH = 4000;

/** The tool's name on a chat card: `mcp__nerve__ask_form` (bare `ask_form` too). */
export function isAskFormTool(tool: string): boolean {
  return tool === 'ask_form' || tool.endsWith('__ask_form');
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function str(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

/** The call's title, '' for none. */
export function formTitle(input: Record<string, unknown> | null | undefined): string {
  return str(input?.title).trim();
}

/**
 * The call's questions, with the contract's defaults where a key is left out
 * (required: true; allow_other: true for choice and multi). Reads the
 * model's own call (`block.input`, which may omit keys) as well as the
 * server's normalized copy of it on the pending interaction.
 */
export function formQuestions(input: Record<string, unknown> | null | undefined): FormQuestion[] {
  let raw: unknown = input?.questions;
  // A model now and then sends the list as a JSON string.
  if (typeof raw === 'string') {
    try {
      raw = JSON.parse(raw);
    } catch {
      return [];
    }
  }
  if (!Array.isArray(raw)) return [];
  const seen = new Set<string>();
  const questions: FormQuestion[] = [];
  for (const entry of raw) {
    if (!isRecord(entry)) continue;
    const id = str(entry.id);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    const kind = (KINDS.has(str(entry.kind)) ? entry.kind : 'text') as FormKind;
    const offersOptions = kind === 'choice' || kind === 'multi';
    const label = str(entry.label).trim() || id;
    questions.push({
      id,
      kind,
      label,
      question: str(entry.question).trim() || label,
      help: str(entry.help).trim(),
      placeholder: str(entry.placeholder),
      required: entry.required !== false,
      options: offersOptions ? optionsOf(entry.options) : [],
      allowOther: offersOptions && entry.allow_other !== false,
      defaultValue: defaultOf(entry.default),
    });
  }
  return questions;
}

function optionsOf(raw: unknown): FormOption[] {
  if (!Array.isArray(raw)) return [];
  return raw.flatMap((option): FormOption[] => {
    if (typeof option === 'string') return option.trim() ? [{ label: option.trim(), description: '' }] : [];
    if (!isRecord(option)) return [];
    const label = str(option.label).trim();
    return label ? [{ label, description: str(option.description).trim() }] : [];
  });
}

function defaultOf(raw: unknown): string | string[] | null {
  if (typeof raw === 'string') return raw;
  if (Array.isArray(raw)) return raw.filter((v): v is string => typeof v === 'string');
  return null;
}

/** Both lists ask the same questions: the same ids, in the same order. */
export function sameIds(a: FormQuestion[], b: FormQuestion[]): boolean {
  return a.length === b.length && a.every((q, i) => q.id === b[i].id);
}

// ------------------------------------------------------------------ //
//  Items (list / repos)                                                //
// ------------------------------------------------------------------ //

/** A repository as the server takes it: owner/name. */
export const REPO_PATTERN = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/;

export function isValidRepo(item: string): boolean {
  return REPO_PATTERN.test(item);
}

const GITHUB_URL = /^(?:https?:\/\/)?(?:www\.)?github\.com\/([^/\s]+)\/([^/\s]+?)(?:\.git)?\/?$/i;
const GITHUB_SSH = /^git@github\.com:([^/\s]+)\/([^/\s]+?)(?:\.git)?$/i;

/**
 * A pasted GitHub address of a repository (`https://github.com/org/repo-a`,
 * `git@github.com:org/repo-a.git`) as owner/name; anything else as typed,
 * trimmed, to be checked against {@link REPO_PATTERN}.
 */
export function normalizeRepo(raw: string): string {
  const item = raw.trim();
  const match = GITHUB_URL.exec(item) ?? GITHUB_SSH.exec(item);
  return match ? `${match[1]}/${match[2]}` : item;
}

/** Text split into items at commas and line breaks, trimmed, blanks dropped. */
export function splitItems(text: string): string[] {
  return text.split(/[,\r\n]+/).map(s => s.trim()).filter(Boolean);
}

/**
 * `items` with `incoming` added: trimmed, blanks and duplicates dropped
 * (repositories compared without case, as GitHub compares them; a GitHub
 * address becomes owner/name).
 */
export function addItems(items: string[], incoming: string[], kind: FormKind): string[] {
  const repos = kind === 'repos';
  const keyOf = (item: string) => (repos ? item.toLowerCase() : item);
  const next = [...items];
  const keys = new Set(next.map(keyOf));
  for (const raw of incoming) {
    const item = (repos ? normalizeRepo(raw) : raw.trim()).slice(0, MAX_ANSWER_LENGTH);
    if (!item || keys.has(keyOf(item))) continue;
    keys.add(keyOf(item));
    next.push(item);
  }
  return next;
}

// ------------------------------------------------------------------ //
//  URL                                                                 //
// ------------------------------------------------------------------ //

/** What the server takes: http:// or https://, a host, no whitespace. */
const URL_PATTERN = /^https?:\/\/[^\s/?#]+\S*$/;

export function isValidUrl(text: string): boolean {
  return URL_PATTERN.test(text.trim());
}

/** Still on its way to an address: the start of `https://` or `http://`. */
export function couldBecomeUrl(text: string): boolean {
  const t = text.trim();
  return 'https://'.startsWith(t) || 'http://'.startsWith(t);
}

// ------------------------------------------------------------------ //
//  Drafts and answers                                                  //
// ------------------------------------------------------------------ //

/** One question's answer while it is being given. */
export interface Draft {
  /** choice / multi: the chosen options, by index (choice: at most one). */
  chosen: number[];
  /** choice / multi: Other… is chosen, with these words ('' until typed); null: not chosen. */
  other: string | null;
  /** text / url: the value; list / repos: what is typed and not yet an item. */
  text: string;
  /** list / repos: the items. */
  items: string[];
}

export const EMPTY_DRAFT: Draft = { chosen: [], other: null, text: '', items: [] };

/**
 * A draft holding `value`: a question's default, or (with `display`) an
 * answer read back from the result. A value that matches no option becomes
 * the Other… words — for a default only where Other… is offered.
 */
export function draftFor(q: FormQuestion, value: FormAnswer | null | undefined, display = false): Draft {
  if (value === null || value === undefined) return EMPTY_DRAFT;
  switch (q.kind) {
    case 'choice':
    case 'multi': {
      const chosen: number[] = [];
      const own: string[] = [];
      for (const raw of Array.isArray(value) ? value : [value]) {
        const v = raw.trim();
        if (!v) continue;
        const at = q.options.findIndex(o => o.label === v);
        if (at === -1) own.push(v);
        else if (!chosen.includes(at)) chosen.push(at);
      }
      const other = own.length > 0 && (q.allowOther || display) ? own.join(', ') : null;
      if (q.kind === 'multi') return { ...EMPTY_DRAFT, chosen: chosen.sort((x, y) => x - y), other };
      // One answer: an option, else the person's own words.
      return chosen.length > 0 ? { ...EMPTY_DRAFT, chosen: [chosen[0]] } : { ...EMPTY_DRAFT, other };
    }
    case 'list':
    case 'repos':
      return { ...EMPTY_DRAFT, items: addItems([], Array.isArray(value) ? value : splitItems(value), q.kind) };
    default:
      return { ...EMPTY_DRAFT, text: Array.isArray(value) ? value.join(', ') : value };
  }
}

/** The items a list draft gives: its items and what is still typed. */
function itemsOf(q: FormQuestion, d: Draft): string[] {
  return addItems(d.items, splitItems(d.text), q.kind);
}

/** The answer the draft gives, or null for none. */
export function answerOf(q: FormQuestion, d: Draft): FormAnswer | null {
  switch (q.kind) {
    case 'choice': {
      // Other… replaces the options.
      if (d.other !== null) return d.other.trim() || null;
      const option = d.chosen.length > 0 ? q.options[d.chosen[0]] : undefined;
      return option ? option.label : null;
    }
    case 'multi': {
      // Other… is one more item, after the options.
      const labels = [...d.chosen].sort((x, y) => x - y).flatMap(i => (q.options[i] ? [q.options[i].label] : []));
      const own = d.other?.trim();
      if (own) labels.push(own);
      return labels.length > 0 ? labels : null;
    }
    case 'list':
    case 'repos': {
      const items = itemsOf(q, d);
      return items.length > 0 ? items : null;
    }
    default:
      return d.text.trim() || null;
  }
}

/** The answers to send: every question that has one, by id. */
export function answersOf(questions: FormQuestion[], draftOf: (q: FormQuestion) => Draft): FormAnswers {
  const answers: FormAnswers = {};
  for (const q of questions) {
    const answer = answerOf(q, draftOf(q));
    if (answer !== null) answers[q.id] = answer;
  }
  return answers;
}

/**
 * What keeps a draft from being sent, or null:
 * - `other`: Other… is ticked beside options with nothing typed (multi;
 *   on a choice it simply gives no answer);
 * - `url`: not an http(s) address;
 * - `repos`: an item that is not owner/name;
 * - `missing`: a required question with no answer.
 */
export type DraftProblem = 'other' | 'url' | 'repos' | 'missing';

export function problemOf(q: FormQuestion, d: Draft): DraftProblem | null {
  if (q.kind === 'multi' && d.other !== null && !d.other.trim()) return 'other';
  if (q.kind === 'url' && d.text.trim() && !isValidUrl(d.text)) return 'url';
  if (q.kind === 'repos' && itemsOf(q, d).some(item => !isValidRepo(item))) return 'repos';
  if (q.required && answerOf(q, d) === null) return 'missing';
  return null;
}

// ------------------------------------------------------------------ //
//  The result                                                          //
// ------------------------------------------------------------------ //

/** A tool result's text: its MCP content blocks' text, or the string itself. */
export function resultText(result: string): string {
  try {
    const parsed: unknown = JSON.parse(result);
    if (Array.isArray(parsed)) {
      return parsed
        .filter((b): b is { type: 'text'; text: string } => isRecord(b) && b.type === 'text' && typeof b.text === 'string')
        .map(b => b.text)
        .join('\n');
    }
  } catch {
    // Not JSON: the text itself.
  }
  return result;
}

export interface ParsedForm {
  answers: FormAnswers;
  skipped: string[];
}

/**
 * The answers in a successful result: its JSON line, or else its readable
 * lines (matched by each question's label). Null when the result holds
 * neither.
 */
export function parseFormResult(result: string | undefined, questions: FormQuestion[]): ParsedForm | null {
  if (!result) return null;
  const text = resultText(result);
  return fromJson(text) ?? fromLines(text, questions);
}

function answersFrom(raw: string): ParsedForm | null {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!isRecord(value) || !isRecord(value.answers)) return null;
  const answers: FormAnswers = {};
  for (const [id, answer] of Object.entries(value.answers)) {
    if (typeof answer === 'string') answers[id] = answer;
    else if (Array.isArray(answer)) answers[id] = answer.filter((v): v is string => typeof v === 'string');
  }
  const skipped = Array.isArray(value.skipped) ? value.skipped.filter((v): v is string => typeof v === 'string') : [];
  return { answers, skipped };
}

function fromJson(text: string): ParsedForm | null {
  // The server writes the JSON last, on one line; from each line that
  // opens an object to the end also reads a pretty-printed one.
  const lines = text.split('\n');
  for (let i = lines.length - 1; i >= 0; i--) {
    if (!lines[i].trimStart().startsWith('{')) continue;
    const parsed = answersFrom(lines[i]) ?? answersFrom(lines.slice(i).join('\n'));
    if (parsed) return parsed;
  }
  return null;
}

const SKIPPED_LINE = new Set(['', '(skipped)', '—', '-']);

function fromLines(text: string, questions: FormQuestion[]): ParsedForm | null {
  const lines = text.split('\n');
  const answers: FormAnswers = {};
  const skipped: string[] = [];
  let found = false;
  let from = 0;
  for (const q of questions) {
    // The server writes the label with its whitespace collapsed.
    const prefix = `${q.label.split(/\s+/).join(' ')}:`;
    const at = lines.findIndex((line, i) => i >= from && line.startsWith(prefix));
    if (at === -1) continue;
    found = true;
    let value = lines[at].slice(prefix.length).trim();
    let next = at + 1;
    // A multi-line answer continues on lines indented by two spaces.
    for (; next < lines.length && lines[next].startsWith('  '); next++) value += `\n${lines[next].slice(2)}`;
    from = next;
    if (SKIPPED_LINE.has(value)) {
      skipped.push(q.id);
    } else if (q.kind === 'multi' || q.kind === 'list' || q.kind === 'repos') {
      answers[q.id] = value.split(', ').map(s => s.trim()).filter(Boolean);
    } else {
      answers[q.id] = value;
    }
  }
  return found ? { answers, skipped } : null;
}

/** The first non-blank line of a text, trimmed. */
export function firstLine(text: string): string {
  return text.split('\n').map(l => l.trim()).find(Boolean) ?? '';
}
