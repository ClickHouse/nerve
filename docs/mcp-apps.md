# MCP Apps

An MCP server can ship an interactive view for a tool: a chart, a form, a
dashboard. This is the MCP extension `io.modelcontextprotocol/ui` (MCP Apps,
specification version 2026-01-26). The tool names a `ui://` resource, the
resource is an HTML document, and a host that supports the extension shows
that document next to the tool call and feeds it the call's input and result.

With `mcp_apps.enabled`, the Nerve web chat is such a host. When the agent
calls a tool of a third-party MCP server and the tool is linked to a view, the
view appears inside the message, in a sandboxed frame. The view can call the
tools of its own server through Nerve.

Off by default. Claude backend only: a Codex session shows the tool call
without a view. Local mode only: in
[external mode](config.md#authentication-mode) the feature stays off.
Telegram and Slack never show a view.

## Enabling

```yaml
# config.yaml
mcp_apps:
  enabled: true
```

Then restart Nerve. The key is in the
[configuration reference](config.md#mcp-apps).

- No other port is opened. The page that the frame of every view loads, the
  sandbox document, is served by the gateway itself at
  `/mcp-app-sandbox.html` of the address the web UI is at. There is one
  address to open, and with a self-signed certificate one exception to
  accept.
- A proxy in front of Nerve needs no rule for it. It must pass the path and
  the gateway's response headers through as they are: the
  `Content-Security-Policy` header of that page is what keeps a view apart
  from the web UI (see [Security model](#security-model)).
- The document is part of the built web UI (`web/dist/mcp-app-sandbox.html`).
  Without it the path answers 503 and the log says to build the web UI
  (`cd web && npm ci && npm run build`, or `nerve upgrade`).

MCP servers need no change in Nerve's configuration: a server that offers
views declares them itself (see [MCP Servers](config.md#mcp-servers)).

## How it works

1. **The CLI is told to host.** Each Claude CLI that Nerve starts gets
   `CLAUDE_CODE_MCP_APPS_HOST=1`. It then tells the MCP servers that the
   client shows `ui://` views, and keeps tools that are for a view only
   (`_meta.ui.visibility: ["app"]`) out of the model's tool list.
2. **A tool call that opens a view is marked.** For a call of
   `mcp__<server>__<tool>`, Nerve looks the tool up in the CLI's MCP status.
   When its `_meta.ui.resourceUri` is a `ui://` URI, the `tool_use` event and
   the stored tool block carry `mcp_app` (server, tool, resource URI). The
   lookup is cached per CLI process, and a call whose lookup fails or is late
   is shown as an ordinary tool call.
3. **Its result is kept as the view needs it.** The `tool_result` event and
   the stored block carry `mcp_app_result`, the result as an MCP
   `CallToolResult`. A tool that reports an error has one too, with
   `isError`. A call that the person stopped or refused has none: the tool
   gave no result, and the view is told that the call was cancelled. The
   block is part of the message, so the view is there again after a reload.
4. **The web UI builds the frame.** It loads the sandbox document from the
   gateway's own address (`/mcp-app-sandbox.html`, a page without an origin),
   reads the view's HTML through `GET /api/sessions/{id}/mcp-apps/resource`,
   and hands it to the sandbox document, which puts it into an inner frame.
5. **The view talks to its server through Nerve.** A `tools/call` from the
   view becomes `POST /api/sessions/{id}/mcp-apps/tool-call`, which the
   session's CLI carries out with no model turn.

The routes and the event fields are in the [API reference](api.md#mcp-apps).

## What a view can do

The web UI shows a view under the card of its tool call, and starts it when
the card comes near the screen. The frame is as wide as the message and as
high as the view says, between 60 and 800 pixels. The view is told the
arguments of the call, then its result (or that the call ended without one),
and what is around it: the theme, light or dark, with Nerve's colours as the
style variables of the specification, the locale, the time zone and the width
it has. A change of theme reaches it while it runs.

What a view asks for is decided by the web UI:

| Request | Answer |
|---------|--------|
| `tools/call` | A tool of the view's own server that is visible to views. One that is for views only, or that the server marks read-only, runs at once. Any other opens a dialog with the tool's name and arguments: Allow once, or Deny. |
| `resources/read` | A `ui://` resource of the view's own server. Nothing else. |
| `ui/open-link` | An http(s) address. A dialog shows it in full; with consent it opens in a new tab that has no handle on the chat. |
| `ui/message` | A dialog shows the whole text: at most 4000 characters, with hidden characters and runs of empty lines taken out. With consent it goes into the message field, if that is empty, and the focus stays where it is. The person sends it, or not. While the message field holds text, the request is refused without a question. At most one request a second. |
| `ui/request-display-mode` | Inline, whatever was asked. |
| `notifications/message` | Written to the browser console (debug). |

A view gets the keyboard when the person presses in it or moves into it with
the Tab key, and in no other way: one that takes it by script, while the
person is elsewhere, is stopped, and its block says "The … app was stopped:
it took the keyboard without a click in it." with Retry. The rule, what it
costs (a press on a view that the pointer has not moved onto only wakes it)
and what it does not stop are those of a widget: see
[The keyboard](widgets.md#the-keyboard).

The dialogs are the chat's own, drawn over the page. One question of a view
is on screen at a time, and the others wait behind it. Its buttons take no
click in its first second: one question takes the place of another in the
same spot, and the second click of a double click must not answer a question
that nobody has read. After three refusals in a row, a view's requests of
that kind (tool calls, links, text) are refused without a question until the
view is loaded again. A dialog that is left open closes when the view stops
waiting for the answer (its SDK waits a minute by default): nothing is called
for a request that was given up.

Of the browser features a view may ask for (`_meta.ui.permissions`: camera,
microphone, location, clipboard), only writing to the clipboard is passed on.
A source on Nerve's own host is dropped from every list of a view's declared
origins.

A view's document has an origin of its own, shared with nothing (an opaque
origin: its frame is sandboxed without `allow-same-origin`). So a view has no
`localStorage`, no `sessionStorage`, no IndexedDB and no cookies (reading
them throws), its requests carry `Origin: null`, and it cannot open a window.
A view that needs storage does not work in Nerve. Chrome runs such a frame in
a process of its own, and the sandbox document around it, which has no origin
either, with it: apart from the process of the chat.

Nerve reaches the MCP servers only through the session's own Claude CLI, with
three of its control requests: `mcp_status`, `mcp_read_resource` and
`mcp_call`. The Python SDK has no methods for the last two, so the Claude
client sends them through the SDK's private control channel, as it does for
an interrupt that cancels queued work. `tests/test_mcp_apps.py` pins what it
relies on.

## Security model

A view is HTML and script from a third party. It is treated as untrusted
throughout.

- **A page of Nerve's own address, without an origin.** The frame's
  document, the sandbox document, comes from the gateway itself, at one
  path, with `Content-Security-Policy: sandbox allow-scripts allow-forms; …`.
  A browser gives a document that is served like that no origin (an opaque
  one: it reads its origin as `"null"`), in a frame and opened as a page of
  its own. So nothing in the frame can read the storage of the web UI, where
  the session token is, nor a cookie, nor the web UI's DOM; a request it
  makes to Nerve's address counts as another site's; it opens no window and
  navigates no page but itself. The token is never in the frame's URL and
  never sent to it. The view itself runs one frame further in, with an
  origin that it shares with nothing: it reaches neither the sandbox document
  nor the frame of another view. Whatever the web UI asks for, the sandbox
  document makes no other kind of frame, and gives it scripts and forms at
  most.
- **The sandbox document runs only without an origin.** `sandbox`, the
  limits on what a view may load, and the rule on who may frame the document
  are one header. A sandbox document that finds itself with an origin did not
  get that header: it shows a notice, tells the chat why ("served without its
  sandbox policy"), and takes no HTML. The chat's frame has no `sandbox`
  attribute of its own for this reason: it would take the document's origin
  away whatever the gateway sent, and a header that did not arrive would go
  unseen, with the view's network limits gone.
- **The path is never answered without the header.** One route answers for
  `/mcp-app-sandbox.html`, and every answer of it carries the header: the
  document, the 404 while the feature is off, the 405 for another method,
  the 503 while the web UI is not built. The static files of the web UI
  never serve the file, however its path is spelled: they answer with the
  web UI's own page instead.
- **Only a page of Nerve's own address may frame it.** The header ends with
  `frame-ancestors 'self'`. A page of another site cannot hold the sandbox
  document in a frame. Neither can a view, nor the sandbox document itself:
  a page without an origin matches no address.
- **A frame is known by its window.** `"null"` is what every sandboxed
  frame says its origin is, so it names nobody. The web UI takes a message
  for a view only from the window of the frame it made for that view, and
  only while that window holds a document without an origin. A view that
  posts to the chat page itself, or a frame that a view made, comes from
  another window and is ignored. In the other direction a document without
  an origin cannot be named as the target of a message, so what the web UI
  posts goes to whatever document its frame holds. Only the web UI puts a
  document there, and what it posts is the view's own: its HTML, the theme
  and the room it has, the arguments and the result of its tool call, the
  answers to its own requests. The sandbox document takes a message from the
  chat only from the window that holds it, at the address it was itself
  loaded from.
- **A view speaks for itself only.** A request reaches the web UI through
  the frame of the view that made it, and is answered for that view's server.
  A view cannot script another view, so it cannot ask in another view's name.
- **The policy is a response header, and the web UI chooses the address.**
  The gateway builds the header from the `csp` parameter of the address it
  is asked for, and the web UI puts into that address what the view's
  resource declares (`_meta.ui.csp`). The gateway cannot tell who asks, so
  the policy holds as long as a view cannot get its frame loaded again at an
  address of its own making. A view cannot: it has no way to script or
  navigate the sandbox document, and the web UI hands a view's HTML to a
  sandbox document once: a frame that says it is ready a second time ends
  the view. Without a declaration no origin is allowed at all. Only what
  reads as an origin is accepted from a declaration: an entry with a quote, a
  semicolon, a comma, a space or a line break is dropped, and so is one that
  allows every host (`*`, a scheme alone, `https://*.com`). No parameter
  changes `sandbox` or `frame-ancestors`.
- **No policy reaches Nerve itself.** The sandbox document is a page of
  Nerve's own address, so this is what keeps a view's requests away from the
  web UI and the API. The policy has no `'self'` in any directive that loads
  or navigates: for a document without an origin `'self'` is the address it
  was loaded from, which is the gateway. And a source on Nerve's own host is
  dropped from every directive, whatever its scheme and port: under every
  name Nerve knows as its own, the API, where the sign-in is, cannot be
  called, and the web UI and a second sandbox document cannot be framed. A
  form goes where a frame may be loaded from, and nowhere without a
  declaration. Nerve's own host is the one the browser asked for, the one a
  proxy in front says it was asked for (`X-Forwarded-Host`), the host of the
  page that holds the frame (the web UI sends its origin with the frame's
  request), the address the gateway is bound to and, on a loopback address,
  every loopback name (`localhost` and `127.0.0.1` are one gateway). The web
  UI drops the sources on the host it is shown at before it asks. Another
  name of the same machine is not recognised: see [Limits](#limits) for what
  follows from that.
- **The web UI refuses every frame.** While MCP Apps is on, the documents of
  the web UI are served with `Content-Security-Policy: frame-ancestors
  'none'`, so no page can show the web UI inside itself, a view least of all.
  (Nothing of Nerve's does. With MCP Apps off the header is not sent.)
- **Nothing goes to the agent in the person's name without the person.** A
  view cannot send a message. It can ask to put text into the message field,
  and the person sees all of that text first (see the table above).
- **A view calls only tools that are for views.** The tool-call route refuses
  a tool whose visibility lacks `app` (403), and it knows no server named
  `nerve`: Nerve's own tools cannot be reached from a view. The web UI adds
  its own rules on top: a view calls only its own server, and a tool that is
  neither read-only nor for views only needs the person's consent.
- **The view's HTML is data.** It travels as a JSON string to the web UI,
  which puts it into the sandbox frame and nowhere else.
- **Sizes and waits are bounded.** A view document is at most 5 MiB. A result
  kept for a view is at most 2 MiB: beyond that `structuredContent` is
  dropped and `_meta` says `{"nerve/truncated": true}`. The arguments of a
  tool call from a view are at most 1 MiB. Reading the MCP status waits at
  most 5 seconds, reading a document 30, and a tool that a view calls is
  ended after 60.

What the first points rest on:

- **The header arrives.** The separation of a view from the web UI is a
  response header of one page. A proxy that drops or replaces
  `Content-Security-Policy`, or a cache that answers for that path with
  something else, takes it away. The sandbox document then refuses to run,
  so a view fails with its reason rather than run unseparated. What the
  document cannot see is a header that still says `sandbox` and has lost the
  rest: a view would then load what it likes (a widget is still held by the
  policy in its own document). A second `Content-Security-Policy` header
  that a proxy adds takes nothing away: the browser applies both.
- **The gateway answers for the path.** The web UI takes the document in
  its frame for the sandbox document when that document has no origin and
  says it is ready; a document without an origin has no name to check. So
  a proxy in front must not answer `/mcp-app-sandbox.html` with a page of
  its own, nor redirect it. Nothing else can put a document there: a view
  cannot navigate the frame above it, and neither can a page of another
  site.
- **The browser honours `sandbox` in that header.** Current browsers do. In
  one that did not, the sandbox document would have an origin and refuse.
- **A request from a page without an origin carries no credential.** The
  web UI authenticates with a token that it keeps in its own storage and
  sends in a header, and a view cannot read it. Where a session is a cookie,
  whether a view's request to Nerve's address would carry it depends on the
  cookie: one with `SameSite=Lax` or `Strict` is not sent (the request counts
  as another site's), one with `SameSite=None` is. The policy above stops a
  view from making the request in the first place; the cookie is the second
  line, and only when it says `Lax` or `Strict` itself (browsers differ on a
  cookie that says nothing).

Two deployments are outside this:

- External mode, where a gateway in front of Nerve signs in every request to
  the address with a session cookie. Nerve keeps MCP Apps off there and logs
  a warning at startup.
- A deployment that signs in with a `nerve_token` cookie. Nerve sets none,
  but the API accepts one when a request names no token itself. Do not
  enable MCP Apps there.

The API reads the `Authorization` header (or the `token` parameter) before
the cookie. Read first, a `nerve_token` cookie that some page of the host
had set would stand in for the valid token of every request of the web UI,
and lock the person out until they delete it. A view cannot set a cookie;
another page on the host name can.

## Limits

- Claude backend only, and only the orchestrator's own tool calls: a
  sub-agent's call of the same tool shows no view.
- A view needs the session's CLI process to call a tool. A session without
  one (after a restart, or once it was idle for long) still shows its views,
  from the documents Nerve keeps on disk (the 64 most recent, in
  `<NERVE_HOME>/cache/mcp_apps`), with the stored input and result. A kept
  document is used only for a server that `mcp_servers` still has, enabled:
  take a server out and its views stop, in old chats too. (So the view of a
  server that a Claude Code plugin brings shows again only once its session
  runs.) Tool
  calls answer 409 until the session runs a turn again: the view is told
  that the session is asleep. Without a kept document the block says "The
  session is asleep" and offers Retry, which works once a message has woken
  the session. A view that calls tools does not keep the session's process
  alive.
- A tool that a view calls sees no permission prompt and no hook of the CLI.
  Consent is the web UI's.
- A call of the agent that the CLI refused under one of its permission rules
  looks like a tool that reported an error: the view gets a result with
  `isError` and the words of the refusal. Only a call that the person
  stopped is told to the view as cancelled. (Nerve's own permission checks
  never refuse a tool of a third-party server.)
- The result of a tool call in a turn is what the CLI reports for it: when
  the tool returns structured content, the text content is the model's copy
  of it (the structured content as JSON), not the server's own text. A tool
  that a view calls itself gets the server's own content blocks.
- The link between a tool and its view is read from the CLI's MCP status and
  kept for the life of the CLI process. A server that changes it later may be
  seen only by the next process.
- Inline display only. A view cannot change the model's context
  (`ui/update-model-context`), ask for a model completion, or register tools
  of its own.
- IPv6 literals in a view's declared origins are dropped.
- **Another name of this machine.** A server can declare, among the origins
  of a view, a DNS name that it controls and that resolves to this machine
  (to `127.0.0.1`, or to the address Nerve listens on). Nerve knows its own
  host by the names above and cannot know that such a name is the same
  machine, so the name goes into the view's policy, and the view's requests
  under it reach the gateway: the API and the WebSocket, without the token.
  Every protected route answers 401. The view then reaches the routes that
  need no sign-in, the sign-in route among them. A document that a view
  frames from an origin it declared (`frameDomains`) runs under no policy of
  Nerve's at all and can ask the gateway in the same way, by any name.
  (Where Nerve listens on a loopback address only, Chrome refuses that to a
  document that came from the public internet.) Use a password that is not
  guessed, and connect only MCP servers that you would let reach those
  routes.
- **A prerender link.** `<link rel="prerender">` in a view makes Chrome load
  any address in the background, past every policy, with data of the view's
  choosing in the address. Nothing on Nerve's side stops it. What it
  reaches and what it carries: [Widgets, Limits](widgets.md#limits).
- Checked in Chrome. The design leans on what other current browsers have
  too (`sandbox` in a Content-Security-Policy header, `frame-ancestors`,
  `SameSite` cookies), but neither Firefox nor Safari was run.

## Troubleshooting

| Sign | Cause |
|------|-------|
| `MCP Apps stays off: it is for local mode only …` (warning) | The authentication mode is external. |
| `the sandbox document … is missing` | The web UI is not built, or the build in `web/dist` is of a version of Nerve without MCP Apps. The path answers 503 until it is built. |
| "The app sandbox does not answer" | The frame got no sandbox document in 8 seconds: `/mcp-app-sandbox.html` of the web UI's own address is not answered by this gateway (a proxy in front answers for it, or the web UI is not built). Retry asks again. |
| "The app sandbox refused to start: this page was served without its sandbox policy" | The sandbox document arrived without the gateway's `Content-Security-Policy` header: something between the gateway and the browser dropped or replaced it. Nothing of a view runs until the header comes through. |
| "The app sandbox refused to start: the embedding page is not the chat of this address" | The sandbox document was put into a frame by a page other than the web UI. |
| "The … app was stopped: it took the keyboard without a click in it" | The view's script took the keyboard for its frame while the person had not pressed in the view. Retry loads it again. See [The keyboard](widgets.md#the-keyboard). |
| A click on a view does nothing, the next one works | The first press woke the view: the pointer had not moved onto it since the person typed. |
| "The session is asleep" | The session has no CLI process (a restart, a long idle time) and Nerve kept no copy of the view's document. Send a message, then press Retry. |
| `GET /api/mcp-apps/config` says `enabled: false` | One of the above, or the configuration was changed without a restart. |
| A tool call shows no view | The server did not link the tool to a `ui://` resource, the session is a Codex session, or the session's CLI was started before the feature was turned on. |
