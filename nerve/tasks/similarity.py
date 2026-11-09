"""Title similarity for task duplicate detection.

``task_create`` has to answer one question before it writes anything: is
there already a task for this? The answer used to come from an FTS5 query
over every task's title *and body* — any word of the new title, as a prefix,
ranked by BM25 against a fixed cut-off. BM25 is a sum over matched terms, so
it grows with the length of the query and with the size of the corpus; the
cut-off that separated duplicates from noise on a hundred short tasks sat
below the score of a single shared rare word once the corpus held thousands
of long-titled tasks with multi-kilobyte bodies, and every new task was
refused as a duplicate.

This module scores titles against titles with IDF-weighted token overlap:

* a token's weight is ``log((N + 1) / (df + 0.5))`` over the title corpus,
  so the words that appear in every title of an install (a project name, a
  ticket prefix, "fix", "CI") weigh nothing and the words that identify a
  task (a test name, an issue number, a function) weigh the most;
* a candidate's score is the larger of *containment* — how much of the new
  title's weight it shares — and the *Dice coefficient* — the shared weight
  against both titles' weight. Both are 0..1 and do not depend on the size
  of the corpus or the length of the title, so one threshold holds across
  installs. Containment catches a short restatement of a long task;
  Dice keeps a long title from matching everything that shares its
  template words.

Dates, bare small numbers and ``HHMMZ`` time stamps are not tokens: they
mark *when* a task was written, not *what* it is, and templated titles
carry them on every line.

Overlap misses one kind of duplicate: two titles about the same test, issue
or function that otherwise say different things ("CI: perf regression on
load_from_file_function in run #16" and "Perf noise: stabilize
load_from_file_function"). One shared token that looks like an identifier and
appears in almost no other title is evidence on its own, so a candidate that
shares a *rare identifier* is returned whatever its score, with the
identifier named (:attr:`SimilarTask.shared_identifiers`).
"""

from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

_WORD_RE = re.compile(r"\w+")
_YEAR_RE = re.compile(r"^(?:19|20)\d\d$")
# "0930z" / "1745z": the UTC time stamps that notification-derived titles carry.
_TIME_STAMP_RE = re.compile(r"^\d{3,4}z$")

# A shared token that names a thing (a test, an issue, a symbol) and appears
# in this many titles or fewer is evidence by itself. Measured on a 12.6K-task
# corpus against 1,401 labeled pairs: at 5 the rule surfaces 13 of 17 true
# duplicates (11 at 3, 15 at 8) while listing 8% of merely related pairs and
# 2% of unrelated ones (6%/2% at 3, 10%/2% at 8).
RARE_IDENTIFIER_DF = 5

# Function words that carry no topic. The IDF weighting would nearly zero them
# anyway; dropping them keeps the shared-token lists readable.
_STOP_WORDS = frozenset({
    "a", "an", "the", "is", "are", "was", "be", "at", "by", "on", "in", "to",
    "of", "for", "and", "or", "not", "it", "its", "as", "do", "if", "so", "no",
    "up", "my", "we", "he", "me", "with", "from", "that", "this", "when",
    "into", "via", "after", "before", "over",
})


def title_tokens(title: str) -> frozenset[str]:
    """The topic-bearing tokens of a title, lower-cased.

    Words are ``\\w+`` runs, so identifiers keep their underscores
    (``01234_some_test_name`` is one token, and a distinctive
    one). Dropped: one-character tokens, stop words, numbers shorter than
    four digits (days, months, counts), years, and ``HHMMZ`` time stamps.
    Longer numbers stay: issue and PR numbers identify work.
    """
    tokens: set[str] = set()
    for raw in _WORD_RE.findall(title or ""):
        word = raw.lower()
        if len(word) < 2 or word in _STOP_WORDS:
            continue
        if word.isdigit():
            if len(word) < 4 or _YEAR_RE.match(word):
                continue
        elif _TIME_STAMP_RE.match(word):
            continue
        tokens.add(word)
    return frozenset(tokens)


def is_identifier(token: str) -> bool:
    """Does the token name a thing rather than describe it?

    Test names, issue and PR numbers, symbols and settings carry digits or
    underscores; plain words do not. Four characters or more, so ``v2`` and
    ``o0`` do not count.
    """
    return len(token) >= 4 and ("_" in token or any(ch.isdigit() for ch in token))


@dataclass(frozen=True)
class SimilarTask:
    """One existing task scored against a title."""

    id: str
    title: str
    status: str
    score: float
    """``max(containment, dice)``, 0..1."""
    containment: float
    """The share of the new title's token weight the task's title also has."""
    dice: float
    """Shared token weight against the weight of both titles."""
    shared: tuple[str, ...]
    """Tokens both titles have, most distinctive first."""
    shared_identifiers: tuple[str, ...]
    """The shared tokens that are identifiers appearing in almost no other title."""
    row: dict
    """The task row the index was built from (deadline, source_url, ...)."""


class TitleIndex:
    """IDF-weighted bag-of-words index over task titles.

    Built once from the task rows (``id``, ``title``, and whatever else the
    caller wants back in :attr:`SimilarTask.row`); immutable afterwards, so
    one instance can serve every caller until the corpus changes. Building
    is linear in the number of titles; a query touches only the postings of
    its own tokens.
    """

    def __init__(self, rows: Iterable[dict]):
        self._rows: dict[str, dict] = {}
        self._tokens: dict[str, frozenset[str]] = {}
        document_frequency: Counter[str] = Counter()
        for row in rows:
            task_id = row["id"]
            tokens = title_tokens(row.get("title") or "")
            self._rows[task_id] = row
            self._tokens[task_id] = tokens
            document_frequency.update(tokens)
        size = len(self._rows)
        self._df = document_frequency
        self._idf: dict[str, float] = {
            token: math.log((size + 1) / (count + 0.5))
            for token, count in document_frequency.items()
        }
        # A token no title has is as distinctive as one a single title has.
        self._unseen_idf = math.log((size + 1) / 0.5)
        self._weight: dict[str, float] = {
            task_id: sum(self._idf[token] for token in tokens)
            for task_id, tokens in self._tokens.items()
        }
        self._postings: dict[str, list[str]] = defaultdict(list)
        for task_id, tokens in self._tokens.items():
            for token in tokens:
                self._postings[token].append(task_id)

    def __len__(self) -> int:
        return len(self._rows)

    def __contains__(self, task_id: str) -> bool:
        return task_id in self._rows

    def _query_weight(self, tokens: frozenset[str]) -> float:
        return sum(self._idf.get(token, self._unseen_idf) for token in tokens)

    def _is_rare_identifier(self, token: str) -> bool:
        return is_identifier(token) and 0 < self._df.get(token, 0) <= RARE_IDENTIFIER_DF

    def _scored(
        self, tokens: frozenset[str], query_weight: float, task_id: str, shared_weight: float,
    ) -> SimilarTask:
        row = self._rows[task_id]
        containment = shared_weight / query_weight
        dice = 2 * shared_weight / (query_weight + self._weight[task_id] or 1.0)
        shared = sorted(tokens & self._tokens[task_id], key=lambda t: (-self._idf[t], t))
        return SimilarTask(
            id=task_id,
            title=row.get("title") or "",
            status=row.get("status") or "",
            score=round(max(containment, dice), 3),
            containment=round(containment, 3),
            dice=round(dice, 3),
            shared=tuple(shared),
            shared_identifiers=tuple(t for t in shared if self._is_rare_identifier(t)),
            row=row,
        )

    def similar(
        self, title: str, *, min_score: float = 0.0, limit: int = 10,
        exclude: str | None = None,
    ) -> list[SimilarTask]:
        """Tasks whose title resembles ``title``.

        Only tasks sharing at least one weighted token are candidates;
        ``min_score`` drops the weak ones, except a task that shares a rare
        identifier with the title, which stays whatever its score. Tasks
        sharing a rare identifier come first, then the rest by score;
        ``limit`` caps the result. ``exclude`` leaves one task out — the
        task itself, when the title being scored already exists.
        """
        tokens = title_tokens(title)
        if not tokens:
            return []
        query_weight = self._query_weight(tokens)
        if query_weight <= 0:
            return []
        shared_weight: dict[str, float] = defaultdict(float)
        rare_hits: set[str] = set()
        for token in tokens:
            idf = self._idf.get(token)
            if idf is None:
                continue
            postings = self._postings[token]
            for task_id in postings:
                shared_weight[task_id] += idf
            if self._is_rare_identifier(token):
                rare_hits.update(postings)
        # Score first, build result objects only for what clears the bar:
        # a title made of common words touches thousands of postings.
        matches = []
        for task_id, weight in shared_weight.items():
            if task_id == exclude:
                continue
            score = max(
                weight / query_weight,
                2 * weight / (query_weight + self._weight[task_id] or 1.0),
            )
            if score >= min_score or task_id in rare_hits:
                matches.append(self._scored(tokens, query_weight, task_id, weight))
        matches.sort(key=lambda m: (not m.shared_identifiers, -m.score, m.id))
        return matches[:limit]

    def compare(self, title: str, task_id: str) -> SimilarTask | None:
        """Score one known task against ``title``; ``None`` if it is not indexed."""
        if task_id not in self._rows:
            return None
        tokens = title_tokens(title)
        query_weight = self._query_weight(tokens)
        if query_weight <= 0:
            row = self._rows[task_id]
            return SimilarTask(
                id=task_id, title=row.get("title") or "", status=row.get("status") or "",
                score=0.0, containment=0.0, dice=0.0, shared=(), shared_identifiers=(),
                row=row,
            )
        shared_weight = sum(self._idf[t] for t in tokens & self._tokens[task_id])
        return self._scored(tokens, query_weight, task_id, shared_weight)
