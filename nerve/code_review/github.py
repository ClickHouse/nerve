"""Read-only GitHub acquisition. Incomplete responses fail visibly."""

from __future__ import annotations

import asyncio
import json
from urllib.parse import quote, urlencode


class ObservationError(RuntimeError):
    pass


class GitHub:
    async def get(self, path, *, raw=False):
        # No shell, credentials in gh's environment/config, never command arguments.
        args = ["gh", "api", "--method", "GET", path]
        if raw:
            args += ["-H", "Accept: application/vnd.github.raw+json"]
        process = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=60)
        except BaseException:
            if process.returncode is None:
                process.kill()
            await process.wait()
            raise
        if process.returncode:
            # gh's stderr may contain request data; expose only status categories.
            message = stderr.decode(errors="replace")
            code = next((code for code in ("403", "404", "429", "401", "422", "502", "503") if code in message), "request failed")
            raise ObservationError(f"GitHub GET: {code}; observation is incomplete")
        return stdout.decode(errors="replace") if raw else json.loads(stdout)

    async def pages(self, path, key=None, limit=100):
        result = []
        total = None
        for page in range(1, limit + 1):
            separator = "&" if "?" in path else "?"
            data = await self.get(f"{path}{separator}per_page=100&page={page}")
            if key:
                total = data.get("total_count", total)
                items = data[key]
            else:
                items = data
            if not isinstance(items, list):
                raise ObservationError("GitHub pagination returned a non-list")
            result.extend(items)
            if len(items) < 100:
                if total is not None and len(result) != total:
                    raise ObservationError("GitHub pagination total changed during acquisition")
                return result
        raise ObservationError("GitHub pagination cap reached; observation is incomplete")

    async def search(self, profile):
        query = f"repo:{profile.repository} is:pr is:open draft:false {profile.query}"
        found = {}
        expected = None
        for page in range(1, 11):
            data = await self.get("search/issues?" + urlencode({"q": query, "per_page": 100, "page": page, "sort": "created", "order": "asc"}))
            total = data["total_count"]
            if data.get("incomplete_results") or total > 1000 or (expected is not None and expected != total):
                raise ObservationError("GitHub search is incomplete or changed during pagination")
            expected = total
            for item in data["items"]:
                # Search data is never trusted as a lifecycle observation.
                if item.get("pull_request") and item["repository_url"].endswith("/" + profile.repository):
                    found[item["number"]] = item
            if len(data["items"]) < 100 or page * 100 >= total:
                if len(found) != total:
                    raise ObservationError("GitHub search count does not match unique PRs")
                return sorted(found)
        raise ObservationError("GitHub search exceeded pagination cap")

    async def pr(self, repository, number):
        return await self.get(f"repos/{repository}/pulls/{number}")

    async def snapshot(self, repository, pr):
        number, head, base = pr["number"], pr["head"]["sha"], pr["base"]["sha"]
        root = f"repos/{repository}"
        comments = await self.pages(f"{root}/issues/{number}/comments")
        reviews = await self.pages(f"{root}/pulls/{number}/reviews")
        inline = await self.pages(f"{root}/pulls/{number}/comments")
        files = await self.pages(f"{root}/pulls/{number}/files", limit=31)
        if len(files) != pr["changed_files"]:
            raise ObservationError("GitHub changed file list is truncated or PR changed")
        comparison = await self.get(f"{root}/compare/{base}...{head}?per_page=1")
        statuses = await self.pages(f"{root}/commits/{head}/statuses")
        checks = await self.pages(f"{root}/commits/{head}/check-runs", "check_runs")
        ci = {
            "statuses": [{k: s.get(k) for k in ("id", "context", "state", "description", "target_url")} for s in statuses],
            "checks": [{k: c.get(k) for k in ("id", "name", "status", "conclusion", "html_url", "output")} for c in checks],
        }
        # A new failure with diagnostic context requires a new audit; normal CI
        # transitions only refresh evidence. Ignore old statuses for a context.
        latest = {}
        for status in ci["statuses"]:
            latest.setdefault(status["context"], status)
        failures = [
            {"context": s["context"], "state": s["state"], "description": s["description"]}
            for s in latest.values() if s["state"] in ("failure", "error")
        ] + [
            {"name": c["name"], "conclusion": c["conclusion"],
             "summary": (c.get("output") or {}).get("summary"),
             "text": (c.get("output") or {}).get("text")}
            for c in ci["checks"] if c["conclusion"] in ("failure", "timed_out", "action_required")
        ]
        def context(items, fields):
            return [{**{key: item.get(key) for key in fields},
                     "author_id": (item.get("user") or {}).get("id")} for item in items]
        result = {
            "repository": repository, "number": number, "head": head, "base": base,
            "base_ref": pr["base"]["ref"], "merge_base": comparison["merge_base_commit"]["sha"],
            "title": pr["title"], "body": pr.get("body") or "",
            "comments": context(comments, ("id", "body")),
            "reviews": context(reviews, ("id", "body", "state", "commit_id")),
            "inline_comments": context(inline, ("id", "body", "path", "line", "original_line", "commit_id", "in_reply_to_id")),
            "files": [{k: f.get(k) for k in ("filename", "previous_filename", "status", "sha")} for f in files],
            "ci": ci, "failures": failures,
        }
        fresh = await self.pr(repository, number)
        if (fresh["head"]["sha"], fresh["base"]["sha"], fresh["updated_at"], fresh.get("draft"), fresh["state"]) != (
            head, base, pr["updated_at"], pr.get("draft"), pr["state"],
        ):
            raise ObservationError("PR changed during snapshot; retry at next observation")
        return result

    async def sources(self, snapshot):
        """Full changed files, including old paths for renames. No API patches."""
        result = []
        size = 0
        for file in snapshot["files"]:
            for side, sha, name in (
                ("before", snapshot["merge_base"], file.get("previous_filename") or file["filename"]),
                ("after", snapshot["head"], file["filename"]),
            ):
                if (side == "before" and file["status"] == "added") or (side == "after" and file["status"] == "removed"):
                    continue
                content = await self.get(
                    f"repos/{snapshot['repository']}/contents/{quote(name, safe='/')}?ref={sha}", raw=True,
                )
                size += len(content.encode())
                if size > 100_000_000:
                    raise ObservationError("Full source snapshot exceeds 100 MB; manual scope decision required")
                result.append({"side": side, "sha": sha, "path": name, "content": content})
        return result
