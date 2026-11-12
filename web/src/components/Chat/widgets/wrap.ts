import type { ResolvedTheme } from '../../../stores/themeStore';
import { MAX_WIDGET_DATA_CHARS, MAX_WIDGET_DATA_KEYS, type WidgetItems } from './storage';

/**
 * The document of a widget: what the chat hands to the sandbox frame for a
 * ```widget block.
 *
 * The source is HTML the model wrote: a fragment or a whole document. Nerve
 * puts a head of its own in front of it:
 *
 * - a policy that allows no network at all, in the document itself: the
 *   header of the sandbox document already allows none when nothing is
 *   declared, and a widget declares nothing, so this one holds whatever
 *   becomes of that header;
 * - base styles, so that a widget that styles nothing looks like the chat:
 *   no background, the chat's text colour and font, and the CSS variables an
 *   MCP App gets (`hostStyleVariables`), each a `light-dark()` pair that the
 *   root's `color-scheme` decides;
 * - a script that reports the height of the content whenever it changes,
 *   applies the theme the chat sends, and gives the widget a `localStorage`
 *   that starts with what the widget saved before (`storage.ts`).
 *
 * Everything of Nerve's comes first and is complete before the first
 * character of the source, and nothing comes after it. So no source can
 * break out of its place: a `</script>`, a `</style>` or an unclosed comment
 * in it closes or swallows nothing of the head. The same holds for what the
 * widget saved, which is text of its own choosing: it stands in the head's
 * script as data, with every character escaped that could end the script.
 * The source runs in the same document as the head's script and can undo
 * all of it at run time. That gives it nothing: the frame's limits are the
 * browser's (the `sandbox` attribute, the policy), and the chat takes two
 * things from a widget, each within bounds: its height, and the data it
 * wants kept, which goes back to the same widget and nowhere else.
 */

/** The messages a widget and the chat exchange: the names MCP Apps uses, as plain notifications. */
export const SIZE_CHANGED = 'ui/notifications/size-changed';
export const HOST_CONTEXT_CHANGED = 'ui/notifications/host-context-changed';
/** What the widget's `localStorage` holds now. A name of Nerve's: MCP Apps has no such message. */
export const STORAGE_CHANGED = 'nerve/widget/storage';

/** The widget's document tells the chat of a change of its `localStorage` at most this often. */
export const STORAGE_REPORT_MS = 100;

/** What the frame of a widget may do: run scripts. No forms, no popups, no origin. */
export const WIDGET_SANDBOX = 'allow-scripts';

/**
 * The policy of a widget's document, on top of the one the gateway sends
 * with the sandbox document: inline scripts and styles, images and media
 * and fonts that the source carries in itself, and nothing from any
 * address. A policy in the document can only take away from the one in the
 * header.
 */
export const WIDGET_POLICY = [
  "default-src 'none'",
  "script-src 'unsafe-inline' 'unsafe-eval' blob: data:",
  "style-src 'unsafe-inline'",
  'img-src data: blob:',
  'media-src data: blob:',
  'font-src data:',
  "connect-src 'none'",
  'worker-src blob:',
  "frame-src 'none'",
  "object-src 'none'",
  "base-uri 'none'",
  "form-action 'none'",
].join('; ');

/** How the chat looks, for a widget: the theme and the variables of `hostStyleVariables`. */
export interface WidgetLook {
  theme: ResolvedTheme;
  variables: Readonly<Record<string, string | undefined>>;
}

/** A value that could end the declaration, the rule or the style element it is written into. */
const UNSAFE_IN_STYLE = /[<>{};]/;

/** The variables as declarations of one rule. Only custom properties, only values that stay in their place. */
function declarations(variables: WidgetLook['variables']): string {
  return Object.entries(variables)
    .filter((entry): entry is [string, string] => /^--[\w-]+$/.test(entry[0])
      && typeof entry[1] === 'string' && entry[1] !== '' && !UNSAFE_IN_STYLE.test(entry[1]))
    .map(([name, value]) => `${name}:${value};`)
    .join('');
}

/**
 * The one base rule with weight. A widget hides a part with the `hidden`
 * attribute and gives the same part a `display` in its own CSS (`.row {
 * display: flex }`); the browser's own rule for `hidden` loses to that, and
 * the part shows all the time. As in the reset of a CSS framework, hidden
 * stays hidden.
 */
const HIDDEN_RULE = '[hidden]:where(:not([hidden="until-found"])){display:none !important;}';

/**
 * The base styles. Every rule for an element is inside `:where()`, which
 * gives it no weight: any rule of the widget wins, wherever it stands.
 * (HIDDEN_RULE is the exception.)
 */
function baseStyles({ theme, variables }: WidgetLook): string {
  return `:root{color-scheme:${theme === 'light' ? 'light' : 'dark'};${declarations(variables)}`
    + 'accent-color:var(--color-text-info);}'
    + ':where(html){background:transparent;}'
    + ':where(*){box-sizing:border-box;}::before,::after{box-sizing:border-box;}'
    + HIDDEN_RULE
    + ':where(body){margin:0;padding:4px;background:transparent;color:var(--color-text-primary);'
    + 'font-family:var(--font-sans);font-size:14px;line-height:1.5;overflow-wrap:break-word;'
    + '-webkit-font-smoothing:antialiased;}'
    + ':where(h1,h2,h3,h4){margin:0 0 0.5em;line-height:1.3;font-weight:600;}'
    + ':where(h1){font-size:1.25rem;}:where(h2){font-size:1.125rem;}:where(h3,h4){font-size:1rem;}'
    + ':where(p,ul,ol){margin:0 0 0.75em;}'
    + ':where(a){color:var(--color-text-info);}'
    + ':where(hr){border:0;border-top:1px solid var(--color-border-primary);margin:0.75em 0;}'
    + ':where(code,pre,kbd,samp){font-family:var(--font-mono);}'
    + ':where(button,input,select,textarea){font:inherit;color:inherit;}'
    + ':where(button,input[type=button],input[type=submit],input[type=reset]){padding:4px 10px;'
    + 'border:1px solid var(--color-border-primary);border-radius:var(--border-radius-md);'
    + 'background:var(--color-background-secondary);cursor:pointer;}'
    + ':where(button,input[type=button],input[type=submit],input[type=reset]):where(:hover){'
    + 'background:var(--color-background-tertiary);}'
    + ':where(button):where(:disabled){opacity:0.5;cursor:default;}'
    + ':where(input:not([type=range],[type=checkbox],[type=radio],[type=color],[type=button],'
    + '[type=submit],[type=reset],[type=file]),select,textarea){padding:4px 8px;'
    + 'border:1px solid var(--color-border-primary);border-radius:var(--border-radius-md);'
    + 'background:var(--color-background-primary);}'
    + ':where(table){border-collapse:collapse;width:100%;}'
    + ':where(th,td){padding:6px 10px;text-align:left;border-bottom:1px solid var(--color-border-primary);}'
    + ':where(th){font-weight:600;color:var(--color-text-secondary);}'
    + ':where(svg){max-width:100%;}:where(img,canvas,video){max-width:100%;height:auto;}';
}

/**
 * The head's script. It runs before the source and is written to hold
 * nothing that could end a script element. Six things, each for the widget
 * that means well; a widget that does not can undo them and gains nothing.
 *
 * - The height. It reports the root element's own height, not the
 *   document's scroll height: that one is never less than the frame, so a
 *   widget that got shorter (a folded panel) would keep the frame at its
 *   tallest. Two things are added: what overflows a body of a fixed height
 *   (`height: 100vh`), and the room a horizontal scrollbar takes, which would
 *   otherwise bring a vertical one with it.
 * - The theme the chat sends: the root's `color-scheme`, which every
 *   `light-dark()` variable follows, and the variables themselves.
 * - Links. A frame that follows a link loads nothing (the policy of the
 *   sandbox forbids it) but loses the widget, and in a frame like this one
 *   even a link to a place in the same document does. So no link navigates;
 *   one to a place in the document scrolls there.
 * - Forms. This frame may not submit a form, and the browser then fires no
 *   `submit` event at all, so a widget that calculates in its `submit`
 *   handler would do nothing. The event is fired here in its place, once for
 *   a press of Enter and the click on the form's button that the browser
 *   makes of it, and not at all if the browser did fire one.
 * - A script error. It is said in the widget itself, in one line: the chat
 *   is told of no error of a widget.
 * - Storage. This frame has no origin, and the browser gives such a frame
 *   no storage: reading `localStorage` throws. An object with the same
 *   methods and the same way with named properties stands in its place,
 *   filled with what the widget saved before (SAVED, which `headScript`
 *   puts in), and tells the chat what it holds after a change, at most
 *   every STORAGE_REPORT_MS. More than the widget may keep (LIMIT, which
 *   `headScript` puts in as well, and MAX_WIDGET_DATA_KEYS keys) throws the
 *   error the browser's own throws. `sessionStorage` is the same object
 *   without a past and without a report: it lasts as long as the frame.
 */
const SAVED = '/*saved*/[]';
const LIMIT = '/*limit*/0';
const HEAD_SCRIPT = `(function () {
  'use strict';
  var saved = ${SAVED};
  var room = ${LIMIT};
  var root = document.documentElement;
  var last = -1;
  function report() {
    if (!document.body) return;
    var box = root.getBoundingClientRect();
    var height = Math.ceil(Math.max(box.height, document.body.scrollHeight))
      + Math.max(0, window.innerHeight - root.clientHeight);
    if (height === last) return;
    last = height;
    parent.postMessage({ jsonrpc: '2.0', method: '${SIZE_CHANGED}', params: { width: Math.ceil(box.width), height: height } }, '*');
  }
  if (typeof ResizeObserver === 'function') new ResizeObserver(report).observe(root);
  document.addEventListener('DOMContentLoaded', report);
  window.addEventListener('load', report);

  window.addEventListener('message', function (event) {
    if (event.source !== parent) return;
    var data = event.data;
    if (!data || data.method !== '${HOST_CONTEXT_CHANGED}' || !data.params) return;
    var theme = data.params.theme;
    if (theme === 'light' || theme === 'dark') root.style.colorScheme = theme;
    var variables = data.params.styles && data.params.styles.variables;
    if (!variables || typeof variables !== 'object') return;
    for (var name in variables) {
      if (name.indexOf('--') === 0 && typeof variables[name] === 'string') root.style.setProperty(name, variables[name]);
    }
  });

  document.addEventListener('click', function (event) {
    var link = event.target && event.target.closest ? event.target.closest('a[href], area[href]') : null;
    if (!link) return;
    event.preventDefault();
    var href = link.getAttribute('href') || '';
    var target = href.charAt(0) === '#' && href.length > 1 ? document.getElementById(href.slice(1)) : null;
    if (target) target.scrollIntoView({ block: 'nearest' });
  }, true);

  var submitted = false;
  var waiting = null;
  document.addEventListener('submit', function (event) {
    if (event.isTrusted) submitted = true;
  }, true);
  function submitLater(form, submitter) {
    if (waiting === form) return;
    waiting = form;
    submitted = false;
    setTimeout(function () {
      waiting = null;
      if (submitted || !form.isConnected) return;
      if (!form.noValidate && typeof form.reportValidity === 'function' && !form.reportValidity()) return;
      var event;
      try {
        event = new SubmitEvent('submit', { bubbles: true, cancelable: true, submitter: submitter });
      } catch (error) {
        event = new Event('submit', { bubbles: true, cancelable: true });
      }
      form.dispatchEvent(event);
    }, 0);
  }
  document.addEventListener('click', function (event) {
    var button = !event.defaultPrevented && event.target && event.target.closest
      ? event.target.closest('button, input[type=submit], input[type=image]') : null;
    if (!button || !button.form || button.disabled) return;
    var type = (button.getAttribute('type') || (button.localName === 'button' ? 'submit' : '')).toLowerCase();
    if (type === 'submit' || type === 'image') submitLater(button.form, button);
  });
  document.addEventListener('keydown', function (event) {
    var field = event.target;
    if (event.key !== 'Enter' || event.defaultPrevented || event.isComposing) return;
    if (!field || field.localName !== 'input' || !field.form) return;
    if (/^(button|submit|reset|image|checkbox|radio|file|range|color)$/.test(field.type)) return;
    submitLater(field.form, null);
  });

  function storage(start, limit, changed) {
    var items = new Map();
    var used = 0;
    var own = Object.prototype.hasOwnProperty;
    var i;
    for (i = 0; i < start.length; i++) {
      items.set(start[i][0], start[i][1]);
      used += start[i][0].length + start[i][1].length;
    }
    var api = {
      getItem: function (key) {
        key = String(key);
        return items.has(key) ? items.get(key) : null;
      },
      setItem: function (key, value) {
        key = String(key);
        value = String(value);
        var next = used + value.length + (items.has(key) ? -items.get(key).length : key.length);
        if (next > limit || (!items.has(key) && items.size >= ${MAX_WIDGET_DATA_KEYS})) {
          throw new DOMException("Failed to execute 'setItem' on 'Storage': Setting the value of '"
            + key.slice(0, 80) + "' exceeded the quota.", 'QuotaExceededError');
        }
        if (items.get(key) === value) return;
        items.set(key, value);
        used = next;
        changed();
      },
      removeItem: function (key) {
        key = String(key);
        if (!items.has(key)) return;
        used -= key.length + items.get(key).length;
        items.delete(key);
        changed();
      },
      clear: function () {
        if (items.size === 0) return;
        items.clear();
        used = 0;
        changed();
      },
      key: function (index) {
        index = Math.floor(Number(index)) || 0;
        return index >= 0 && index < items.size ? Array.from(items.keys())[index] : null;
      }
    };
    var object = new Proxy(Object.create(typeof Storage === 'function' ? Storage.prototype : Object.prototype), {
      get: function (target, name) {
        if (name === 'length') return items.size;
        if (typeof name === 'string' && own.call(api, name)) return api[name];
        if (name in target) return target[name];
        return typeof name === 'string' && items.has(name) ? items.get(name) : undefined;
      },
      set: function (target, name, value) {
        if (typeof name === 'string') api.setItem(name, value);
        return true;
      },
      deleteProperty: function (target, name) {
        if (typeof name === 'string') api.removeItem(name);
        return true;
      },
      has: function (target, name) {
        return name in target || (typeof name === 'string' && items.has(name));
      },
      ownKeys: function () {
        return Array.from(items.keys());
      },
      getOwnPropertyDescriptor: function (target, name) {
        return typeof name === 'string' && items.has(name)
          ? { value: items.get(name), writable: true, enumerable: true, configurable: true } : undefined;
      }
    });
    return {
      object: object,
      entries: function () {
        return Array.from(items.entries());
      }
    };
  }
  var told = 0;
  var telling = false;
  var kept = storage(saved, room, function () {
    if (telling) return;
    telling = true;
    setTimeout(function () {
      telling = false;
      told = Date.now();
      parent.postMessage({ jsonrpc: '2.0', method: '${STORAGE_CHANGED}', params: { items: kept.entries() } }, '*');
    }, Math.max(0, told + ${STORAGE_REPORT_MS} - Date.now()));
  });
  var brief = storage([], ${MAX_WIDGET_DATA_CHARS}, function () {});
  saved = null;
  function provide(name, object) {
    try {
      Object.defineProperty(window, name, {
        configurable: true,
        enumerable: true,
        get: function () {
          return object;
        }
      });
    } catch (error) {
      return;
    }
  }
  provide('localStorage', kept.object);
  provide('sessionStorage', brief.object);

  var said = false;
  function fault(message) {
    message = String(message || '');
    if (said || !message || message.indexOf('ResizeObserver loop') === 0) return;
    said = true;
    function show() {
      if (!document.body) return;
      var note = document.createElement('div');
      note.setAttribute('role', 'status');
      note.style.cssText = 'margin-top:8px;padding:4px 8px;border:1px solid var(--color-border-warning);'
        + 'border-radius:6px;background:var(--color-background-warning);color:var(--color-text-warning);'
        + 'font:12px/1.4 var(--font-sans);';
      note.textContent = 'This widget has a script error: ' + message.slice(0, 200);
      document.body.appendChild(note);
    }
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', show);
    else show();
  }
  window.addEventListener('error', function (event) {
    fault(event.message);
  });
  window.addEventListener('unhandledrejection', function (event) {
    var reason = event.reason;
    fault(reason && reason.message ? reason.message : reason);
  });
})();`;

/** A document type at the very start of the source (after blank space or a byte order mark, which `\s` covers). */
const LEADING_DOCTYPE = /^\s*<!doctype[^>]*>/i;

/**
 * A value as JSON that can stand in a script element: the characters that
 * could end the element, open a comment in it, or end a line of script are
 * written as their escapes, which JSON reads back as the same characters.
 * (JSON itself escapes quotes, control characters and half surrogate pairs.)
 */
function scriptJson(value: unknown): string {
  return JSON.stringify(value)
    .replace(/[<>&\u2028\u2029]/g, char => `\\u${char.charCodeAt(0).toString(16).padStart(4, '0')}`);
}

/** The head's script for a widget that saved `saved` before and may keep `limit` characters. */
function headScript(saved: WidgetItems, limit: number): string {
  // Not `replace` with a text: what the widget saved may hold `$&`.
  const [before, between] = HEAD_SCRIPT.split(SAVED);
  const [middle, after] = between.split(LIMIT);
  const room = Number.isFinite(limit) ? Math.max(0, Math.min(MAX_WIDGET_DATA_CHARS, Math.floor(limit))) : 0;
  return before + scriptJson(saved) + middle + String(room) + after;
}

/**
 * The head Nerve puts in front of every widget. Complete in itself: every
 * element it opens, it closes. `saved` is what the widget's `localStorage`
 * starts with, `limit` the characters it may keep.
 */
export function widgetHead(
  look: WidgetLook, saved: WidgetItems = [], limit: number = MAX_WIDGET_DATA_CHARS,
): string {
  return '<!doctype html>'
    + '<meta charset="utf-8">'
    + `<meta http-equiv="Content-Security-Policy" content="${WIDGET_POLICY}">`
    + `<style>${baseStyles(look)}</style>`
    + `<script>${headScript(saved, limit)}</script>`;
}

/**
 * The document for the widget `source`.
 *
 * The head goes first and the source after it, whole. In a fragment the
 * browser's parser puts the head's elements into the `<head>` it makes, and
 * the fragment into the `<body>`. In a whole document the parser has the
 * head open already when it meets the source's `<html>` and `<head>`: it
 * adds the attributes of that `<html>` to the root and carries on in the
 * same head, so the head's elements are the first of the source's own head.
 * The one document type is Nerve's, at the very start; the source's own, if
 * it leads with one, is dropped, since a second one means nothing.
 */
export function wrapWidget(
  source: string, look: WidgetLook, saved: WidgetItems = [], limit: number = MAX_WIDGET_DATA_CHARS,
): string {
  return widgetHead(look, saved, limit) + source.replace(LEADING_DOCTYPE, '');
}
