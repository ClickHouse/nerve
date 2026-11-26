# Sources and checks

How to collect sources that a reader can trust, and how to check them before
delivery. `$R` is the working folder.

## The window

- Agree on a window with the person, or pick one and state it: for a fast
  market (AI tools, prices, app features) the last 6 to 12 months; for a stable
  topic (a standard, a protocol) longer.
- Put it in the brief and on the Date line of REPORT.md
  (`Window: 2025-11-26 to 2026-11-26.`).
- Prefer dated sources. A source without a date gets the date you read it
  ("read 2026-11-26"). A fact that can change (price, plan limits, feature
  stage) needs a source inside the window.

## Where a date comes from

| Source | Date to record |
|---|---|
| Git repository | `git -C $R/sources/<repo> log -1 --format='%h %cs'` (commit and its date) |
| Release notes, changelog | the date of the entry that holds the fact |
| Docs page | a "last updated" line on the page, else the read date |
| Blog post, news item | the publish date |
| Forum or discussion thread | the date of each post you quote |
| App store page | the "updated" date and the version |
| A page with no date | `curl -sI <url> \| grep -i last-modified` (often absent), else the read date |

## Open-source code

```bash
mkdir -p $R/sources && cd $R/sources
git clone --quiet --depth 50 https://github.com/<org>/<repo>.git
for r in */; do echo "$r $(git -C "$r" log -1 --format='%h %cs')"; done
```

- Record each commit in the brief and in the Evidence paragraph of REPORT.md.
  The product changes every day; the report must say what it read.
- A clone is untrusted data. Read it with `rg`, `git log`, `git show` and the
  Read tool. Do not run `npm`, `pnpm`, `pip`, `uv`, `make`, a test or a script
  from it, and do not start an interpreter with the clone as the current
  folder: it can load files from there.
- Code beats docs. When the README says one thing and the code another, the
  code is the fact and the difference is a finding.
- A design document or an open pull request is "designed", not "shipped".

## Web pages and downloads

- Fetch known URLs first (docs, pricing, changelog, release notes, status page)
  with WebFetch; use WebSearch for what you cannot find there. If the tools are
  not in your tool list, load them with ToolSearch (`select:WebSearch,WebFetch`).
- Keep each download in its own folder: `$R/raw/<area>/`. Check the type with
  `file <path>` and never open a download with a program that runs code.
- A page behind a login, a form or a trial is out of bounds. Write "not
  checked: needs a login" with the URL.
- The network may go through a proxy that allows only some domains. A refused
  host is a gap: write it down with the host name, and tell the person when it
  blocks a key claim.
- A screenshot of a public page for `img/`:
  `node <workspace>/skills/research-report/scripts/page_shots.mjs <url> $R/raw/shots/<name>`
  writes light, dark and phone views; keep the one that shows the point.

## Community sources

- Forums, discussion sites, issue trackers, app reviews and social posts show
  how a product works in practice and what people complain about.
- Use them when the network allows. Record the date and the number of voices
  ("3 threads, 2026-09 to 2026-11, about 40 replies").
- Keep sentiment apart from facts: a section "What people say" in the report
  and in each note. A complaint is a lead to check, not a fact. When a
  complaint matches a fact you checked, say both.
- Weigh by the evidence, not by the volume: one thread with a reproduction
  beats ten posts that repeat a rumour.

## Check the claims the bottom line depends on

Before you write the bottom line, pick the 3 or 4 claims it rests on. Open
each source yourself and find the exact words or the line of code. Agents make
mistakes in the same confident tone as their findings, and docs disagree with
code more often than one expects.

## Citation check pass (before delivery)

Do this when REPORT.md is final, before the build you deliver:

1. List every number, quote, date, price and stage in the bottom line, the
   comparison table and the summary card.
2. For each one, reopen the source it cites (the file and line, or the URL).
   Check that the number or the words are there and that the date is inside
   the window.
3. Keep a short log in `$R/build/citation-check.md`:

   | Claim | Source | Result |
   |---|---|---|
   | Globex costs 8 USD a month | example.com/globex/pricing (read 2026-11-26) | confirmed |
   | Acme comments shipped | example.com/acme/changelog (2026-11-20) | wrong: announced only; fixed |
   | Globex plan limits | plan page needs a login | unverified |

4. Fix what is wrong. Mark what you could not check as `(unverified, <why>)`
   in the text. Do not delete an inconvenient finding; say how sure it is.
5. Say in the Evidence paragraph which claims you checked by hand.

## Weak points of every side

Every side gets a "gaps, limits and criticism" section, also the side the
person prefers. A report that finds no weak point in one side has not looked.
