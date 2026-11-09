"""HTTP route tests for ``GET /api/github/pulls`` (gateway/routes/github.py).

No test calls GitHub. Most tests replace ``asyncio.create_subprocess_exec``
in the route module with a fake ``gh`` process. The tests of the process
boundary put a stand-in ``gh`` executable first on ``PATH`` (or no ``gh`` at
all) and run it for real.
"""

from __future__ import annotations

import asyncio
import json
import os
import stat
import sys
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from nerve.gateway.routes import github

URL = "https://github.com/example/repo/pull/12"
URL_2 = "https://github.com/example/repo/pull/13"

GH_ANSWER = {
    "number": 12,
    "title": "Fix the flaky login test",
    "state": "OPEN",
    "isDraft": False,
    "reviewDecision": "APPROVED",
    "headRefName": "alice/fix-login",
    "baseRefName": "main",
    "statusCheckRollup": [
        {"__typename": "CheckRun", "name": "lint", "status": "COMPLETED", "conclusion": "SUCCESS"},
        {"__typename": "CheckRun", "name": "docs", "status": "COMPLETED", "conclusion": "SKIPPED"},
        {"__typename": "CheckRun", "name": "unit", "status": "COMPLETED", "conclusion": "FAILURE"},
        {"__typename": "CheckRun", "name": "e2e", "status": "IN_PROGRESS", "conclusion": ""},
        {"__typename": "CheckRun", "name": "build", "status": "QUEUED", "conclusion": ""},
        # The run of "unit" that a newer run replaced.
        {"__typename": "CheckRun", "name": "unit", "status": "COMPLETED", "conclusion": "CANCELLED"},
        {"__typename": "StatusContext", "context": "ci/legacy", "state": "SUCCESS"},
        {"__typename": "StatusContext", "context": "ci/deploy", "state": "PENDING"},
        {"__typename": "StatusContext", "context": "ci/scan", "state": "ERROR"},
    ],
}


class FakeProcess:
    def __init__(self, stdout: bytes, returncode: int, delay: float):
        self._stdout = stdout
        self._delay = delay
        self.returncode: int | None = None
        self._exit = returncode
        self.killed = False

    async def communicate(self) -> tuple[bytes, bytes]:
        await asyncio.sleep(self._delay)
        self.returncode = self._exit
        return self._stdout, b"GraphQL: Could not resolve to a PullRequest"

    def kill(self) -> None:
        self.killed = True

    async def wait(self) -> int:
        return self._exit


class FakeGh:
    """Stands in for ``asyncio.create_subprocess_exec`` in the route module."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.processes: list[FakeProcess] = []
        self.answers: dict[str, Any] = {}
        self.returncode = 0
        self.delay = 0.0
        self.running = 0
        self.most_running = 0

    async def __call__(self, *argv: str, **kwargs: Any) -> FakeProcess:
        self.calls.append(argv)
        number = argv[3]
        answer = self.answers.get(number, {**GH_ANSWER, "number": int(number)})
        body = answer if isinstance(answer, bytes) else json.dumps(answer).encode()
        process = FakeProcess(body, self.returncode, self.delay)
        fake = self

        original = process.communicate

        async def communicate() -> tuple[bytes, bytes]:
            fake.running += 1
            fake.most_running = max(fake.most_running, fake.running)
            try:
                return await original()
            finally:
                fake.running -= 1

        process.communicate = communicate  # type: ignore[method-assign]
        self.processes.append(process)
        return process


@pytest.fixture(autouse=True)
def _fresh_cache():
    github.reset_cache()
    yield
    github.reset_cache()


@pytest.fixture
def fake_gh(monkeypatch) -> FakeGh:
    fake = FakeGh()
    monkeypatch.setattr(github.asyncio, "create_subprocess_exec", fake)
    return fake


@pytest.fixture
def client(bypass_auth):
    app = FastAPI()
    app.include_router(github.router)
    bypass_auth(app)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://nerve-test")


async def _pulls(client: httpx.AsyncClient, *urls: str) -> httpx.Response:
    return await client.get("/api/github/pulls", params=[("url", u) for u in urls])


@pytest.mark.asyncio
class TestSummary:
    async def test_card_fields_of_an_open_pull_request(self, client, fake_gh):
        res = await _pulls(client, URL)
        assert res.status_code == 200
        assert res.json() == {"pulls": [{
            "url": URL,
            "error": None,
            "pull": {
                "repo": "example/repo",
                "number": 12,
                "title": "Fix the flaky login test",
                "state": "open",
                "review": "approved",
                "checks": {"passed": 3, "failed": 2, "running": 3, "cancelled": 1},
                "head_ref": "alice/fix-login",
                "base_ref": "main",
            },
        }]}

    @pytest.mark.parametrize(("state", "is_draft", "expected"), [
        ("OPEN", True, "draft"),
        ("OPEN", False, "open"),
        ("MERGED", False, "merged"),
        ("CLOSED", False, "closed"),
        ("CLOSED", True, "closed"),
    ])
    async def test_state(self, client, fake_gh, state, is_draft, expected):
        fake_gh.answers["12"] = {**GH_ANSWER, "state": state, "isDraft": is_draft}
        pull = (await _pulls(client, URL)).json()["pulls"][0]["pull"]
        assert pull["state"] == expected

    async def test_no_checks_and_no_review(self, client, fake_gh):
        fake_gh.answers["12"] = {
            **GH_ANSWER, "reviewDecision": "", "statusCheckRollup": None,
        }
        pull = (await _pulls(client, URL)).json()["pulls"][0]["pull"]
        assert pull["review"] is None
        assert pull["checks"] == {"passed": 0, "failed": 0, "running": 0, "cancelled": 0}

    @pytest.mark.parametrize(("conclusion", "bucket"), [
        ("SUCCESS", "passed"),
        ("NEUTRAL", "passed"),
        ("SKIPPED", "passed"),
        ("CANCELLED", "cancelled"),
        ("STALE", "cancelled"),
        ("FAILURE", "failed"),
        ("TIMED_OUT", "failed"),
        ("ACTION_REQUIRED", "failed"),
        ("STARTUP_FAILURE", "failed"),
    ])
    async def test_each_conclusion_of_a_completed_check_run_has_its_bucket(
        self, client, fake_gh, conclusion, bucket,
    ):
        fake_gh.answers["12"] = {**GH_ANSWER, "statusCheckRollup": [
            {"__typename": "CheckRun", "name": "ci", "status": "COMPLETED", "conclusion": conclusion},
        ]}
        pull = (await _pulls(client, URL)).json()["pulls"][0]["pull"]
        none = {"passed": 0, "failed": 0, "running": 0, "cancelled": 0}
        assert pull["checks"] == {**none, bucket: 1}

    async def test_review_decision_is_lower_case(self, client, fake_gh):
        fake_gh.answers["12"] = {**GH_ANSWER, "reviewDecision": "CHANGES_REQUESTED"}
        pull = (await _pulls(client, URL)).json()["pulls"][0]["pull"]
        assert pull["review"] == "changes_requested"

    async def test_gh_gets_the_parsed_parts_as_separate_arguments(self, client, fake_gh):
        await _pulls(client, "https://github.com/example/my.repo_2/pull/7")
        assert fake_gh.calls == [(
            "gh", "pr", "view", "7", "--repo", "github.com/example/my.repo_2",
            "--json", github.GH_FIELDS,
        )]

    async def test_one_result_per_distinct_url_in_request_order(self, client, fake_gh):
        res = await _pulls(client, URL_2, URL, URL_2)
        assert [p["url"] for p in res.json()["pulls"]] == [URL_2, URL]
        assert len(fake_gh.calls) == 2


@pytest.mark.asyncio
class TestValidation:
    @pytest.mark.parametrize("url", [
        "http://github.com/example/repo/pull/12",
        "https://github.com.example.com/example/repo/pull/12",
        "https://gitlab.com/example/repo/pull/12",
        "https://github.com/example/repo/pull/12/files",
        "https://github.com/example/repo/pull/12?x=1",
        "https://github.com/example/repo/pull/12#top",
        "https://github.com/example/repo/pull/12\n",
        " https://github.com/example/repo/pull/12",
        "https://github.com/example/repo/issues/12",
        "https://github.com/example/repo/pull/0",
        "https://github.com/example/repo/pull/012",
        "https://github.com/example/repo/pull/-1",
        "https://github.com/example/repo/pull/12345678901",
        "https://github.com/-example/repo/pull/12",
        "https://github.com/--repo/repo/pull/12",
        "https://github.com/example/../pull/12",
        "https://github.com/example/./pull/12",
        "https://github.com/example/re po/pull/12",
        "https://github.com/example/repo;rm/pull/12",
        "https://github.com/a/b/c/pull/12",
        "--help",
        "",
    ])
    async def test_anything_but_a_pull_request_url_is_refused(self, client, fake_gh, url):
        res = await _pulls(client, URL, url)
        assert res.status_code == 400
        assert fake_gh.calls == []

    async def test_no_url_is_refused(self, client, fake_gh):
        res = await client.get("/api/github/pulls")
        assert res.status_code == 400

    async def test_more_than_ten_urls_are_refused(self, client, fake_gh):
        urls = [f"https://github.com/example/repo/pull/{n}" for n in range(1, 12)]
        res = await _pulls(client, *urls)
        assert res.status_code == 400
        assert fake_gh.calls == []

    async def test_ten_urls_are_accepted(self, client, fake_gh):
        urls = [f"https://github.com/example/repo/pull/{n}" for n in range(1, 11)]
        res = await _pulls(client, *urls)
        assert res.status_code == 200
        assert len(res.json()["pulls"]) == 10


@pytest.mark.asyncio
class TestCacheAndLimits:
    async def test_a_second_request_within_the_cache_time_runs_no_gh(self, client, fake_gh):
        first = await _pulls(client, URL)
        second = await _pulls(client, URL)
        assert second.json() == first.json()
        assert len(fake_gh.calls) == 1

    async def test_after_the_cache_time_gh_runs_again(self, client, fake_gh, monkeypatch):
        clock = [1000.0]
        monkeypatch.setattr(github.time, "monotonic", lambda: clock[0])
        await _pulls(client, URL)
        clock[0] += github.CACHE_SECONDS - 1
        await _pulls(client, URL)
        assert len(fake_gh.calls) == 1
        clock[0] += 1
        fake_gh.answers["12"] = {**GH_ANSWER, "state": "MERGED"}
        res = await _pulls(client, URL)
        assert len(fake_gh.calls) == 2
        assert res.json()["pulls"][0]["pull"]["state"] == "merged"

    async def test_errors_are_cached_too(self, client, fake_gh):
        fake_gh.returncode = 1
        await _pulls(client, URL)
        await _pulls(client, URL)
        assert len(fake_gh.calls) == 1

    async def test_concurrent_requests_share_one_gh_call(self, client, fake_gh):
        fake_gh.delay = 0.05
        first, second = await asyncio.gather(_pulls(client, URL), _pulls(client, URL))
        assert first.json() == second.json()
        assert len(fake_gh.calls) == 1

    async def test_a_request_that_ends_early_does_not_cancel_the_shared_call(self, fake_gh):
        fake_gh.delay = 0.05
        match = github.PULL_URL.fullmatch(URL)
        first = asyncio.ensure_future(github._cached_pull(URL, match))
        second = asyncio.ensure_future(github._cached_pull(URL, match))
        await asyncio.sleep(0.01)
        first.cancel()
        assert (await second)["pull"]["number"] == 12
        assert len(fake_gh.calls) == 1

    async def test_at_most_four_gh_calls_run_at_once(self, client, fake_gh):
        fake_gh.delay = 0.02
        urls = [f"https://github.com/example/repo/pull/{n}" for n in range(1, 11)]
        res = await _pulls(client, *urls)
        assert res.status_code == 200
        assert len(fake_gh.calls) == 10
        assert fake_gh.most_running == github.MAX_CONCURRENT_GH


@pytest.mark.asyncio
class TestErrors:
    async def test_gh_exit_code_is_no_access(self, client, fake_gh):
        fake_gh.returncode = 1
        res = await _pulls(client, URL)
        assert res.json() == {"pulls": [{"url": URL, "pull": None, "error": "no_access"}]}

    async def test_a_gh_call_that_runs_too_long_is_stopped(self, client, fake_gh, monkeypatch):
        monkeypatch.setattr(github, "GH_TIMEOUT_SECONDS", 0.01)
        fake_gh.delay = 1.0
        res = await _pulls(client, URL)
        assert res.json()["pulls"][0]["error"] == "gh_unavailable"
        assert fake_gh.processes[0].killed

    async def test_output_that_is_not_json_is_gh_unavailable(self, client, fake_gh):
        fake_gh.answers["12"] = b"not json"
        res = await _pulls(client, URL)
        assert res.json()["pulls"][0]["error"] == "gh_unavailable"

    async def test_one_error_does_not_hide_the_other_results(self, client, fake_gh):
        fake_gh.answers["13"] = b"[]"
        res = await _pulls(client, URL, URL_2)
        pulls = res.json()["pulls"]
        assert pulls[0]["pull"]["number"] == 12
        assert pulls[1] == {"url": URL_2, "pull": None, "error": "gh_unavailable"}


def _install_gh(directory, script: str) -> None:
    """A stand-in ``gh`` executable that runs ``script`` with this Python."""
    path = directory / "gh"
    path.write_text(f"#!{sys.executable}\n{script}")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


@pytest.mark.asyncio
class TestProcessBoundary:
    async def test_without_gh_on_path_the_result_is_gh_unavailable(self, client, tmp_path, monkeypatch):
        monkeypatch.setenv("PATH", str(tmp_path))
        res = await _pulls(client, URL)
        assert res.json()["pulls"][0] == {"url": URL, "pull": None, "error": "gh_unavailable"}

    async def test_a_real_process_gets_the_arguments_and_its_answer_is_read(
        self, client, tmp_path, monkeypatch,
    ):
        log = tmp_path / "argv.json"
        _install_gh(tmp_path, (
            "import json, sys\n"
            f"open({str(log)!r}, 'w').write(json.dumps(sys.argv[1:]))\n"
            f"print(json.dumps({GH_ANSWER!r}))\n"
        ))
        monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ.get("PATH", ""))
        res = await _pulls(client, URL)
        assert json.loads(log.read_text()) == [
            "pr", "view", "12", "--repo", "github.com/example/repo", "--json", github.GH_FIELDS,
        ]
        assert res.json()["pulls"][0]["pull"]["title"] == "Fix the flaky login test"

    async def test_a_real_process_that_fails_is_no_access(self, client, tmp_path, monkeypatch):
        _install_gh(tmp_path, "import sys\nsys.stderr.write('HTTP 404')\nsys.exit(1)\n")
        monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ.get("PATH", ""))
        res = await _pulls(client, URL)
        assert res.json()["pulls"][0]["error"] == "no_access"


@pytest.mark.asyncio
async def test_the_route_is_registered_and_needs_auth(fake_gh):
    from nerve.config import NerveConfig, set_config
    from nerve.gateway.auth import pin_jwt_secret
    from nerve.gateway.routes import register_all_routes

    secret = "test-secret-for-github-routes-0123456789abcdef"
    set_config(NerveConfig.from_dict({"auth": {"jwt_secret": secret}}))
    pin_jwt_secret(secret)
    try:
        app = FastAPI()
        app.include_router(register_all_routes())
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://nerve-test",
        ) as anonymous:
            res = await _pulls(anonymous, URL)
        assert res.status_code == 401
        assert fake_gh.calls == []
    finally:
        set_config(NerveConfig())
