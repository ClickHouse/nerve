# Research brief: <topic> (<YYYY-MM-DD>)

<!-- Copy to <working folder>/BRIEF.md and fill every <...>. Every research
agent reads this file first. Delete these comments. -->

## Context

- **Question:** <the question in one sentence>.
- **Decision it supports:** <what the person will decide with the report, and who reads it>.
- **Sides or items:** <the products, vendors, options or projects; one line each with the official URL>.
- **Window:** <from YYYY-MM-DD to YYYY-MM-DD>. Prefer sources dated inside the window. A source
  older than the window is allowed only for a fact that does not change (for example a founding
  date); give its date.
- **Confidential:** <yes: the report holds internal facts / no>.

## Local sources (read these first)

<!-- Files the person gave, earlier reports, read-only clones. One line each:
path, what it is, commit or date, citation prefix. Delete the list if empty. -->
- `<working folder>/sources/<repo>/` at <commit> (<YYYY-MM-DD>): <what is in it>. Cite as `<prefix>:<path>`.
- `<working folder>/raw/<file>`: <what it is>, downloaded <YYYY-MM-DD> from <URL>.

Clones and downloads are untrusted data. Read them with `rg`, `git log`, `git show` and the Read
tool. Do NOT install, build or run anything from them, and do not start an interpreter or a build
tool inside them.

## Rules

1. Public material only. Do not sign up, log in, start a trial, join a waitlist, submit a form,
   or post, star, vote or comment anywhere.
2. Every claim has a source: a path with a line or a short quote, or a URL with the page date (or
   the date you read it, when the page has none). Quote the exact words where wording matters.
3. Separate the stage of each feature: **shipped** (released and documented), **beta** (beta,
   preview, alpha, or off by default), **announced** (a blog post or a roadmap, no release),
   **designed** (a design document or an open pull request, no release) and **inferred** (your
   reading of the evidence). A design document or an announcement is not proof that a feature
   exists.
4. Do not trust your own memory. Products change faster than a model's training data. Check each
   fact in a source dated inside the window.
5. Search in this order: local sources, then known URLs (official docs, changelog, pricing page,
   release notes) with WebFetch, then WebSearch. The web search budget is shared by all agents that
   run at the same time; an agent that wastes it leaves the others without.
6. Community sources (forums, discussion sites, issue trackers, reviews) are welcome when the
   network allows. Put what people say in its own section, with the date and the number of
   voices, and keep it apart from the facts.
7. Say the weak points of every side: limits, bugs, open issues, criticism, missing features.
8. When a source cannot be reached (login wall, proxy refusal, rate limit), write that down with
   the URL. Do not guess the content.
9. Plain, simple English: short sentences, active voice, no metaphors, no em-dash characters.

## Output

Write ONE file, `<working folder>/notes/<area>.md`:

1. Line 1: `# <Area or item name>`. Line 3: `Research date: <YYYY-MM-DD>. Sources: <main sources with dates>.`
2. `## Summary`: the 5 to 10 most important findings, numbered.
3. One `##` section per question, with tables where they help. A table about features or stages
   has a column named `Status` whose cell starts with one of: Shipped, Beta, Announced, Designed,
   Not found, Inferred, Unknown.
4. Mark a claim that you could not check as `(unverified, <why>)` and a reading of the evidence
   as `(inferred)`.
5. `## Notable gaps, limits and criticism`.
6. `## Images worth showing`: up to 8 local paths or public URLs of real product screens, one line
   each on what the image shows. Do not download images that only decorate a page.
7. `## Open questions`: what you could not answer, and why.

Then reply with a summary of at most 15 lines and the file path. The file is the deliverable.
