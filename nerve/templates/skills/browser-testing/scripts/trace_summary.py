#!/usr/bin/env python3
"""Print a Playwright trace.zip as text: each action with its time and error,
console errors, page errors and failed requests. Standard library only.

    python3 -I trace_summary.py <trace.zip> [--all]

--all also prints the actions that passed before the last 15.
"""
import json
import sys
import zipfile


def main() -> int:
    paths = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(paths) != 1:
        print(__doc__.strip(), file=sys.stderr)
        return 2
    show_all = "--all" in sys.argv
    try:
        zf = zipfile.ZipFile(paths[0])
    except (OSError, zipfile.BadZipFile) as e:
        print(f"trace_summary: cannot open {paths[0]}: {e}", file=sys.stderr)
        return 2

    events, network = [], []
    for name in zf.namelist():
        if name.endswith(".trace") or name.endswith(".network"):
            for line in zf.read(name).decode("utf-8", "replace").splitlines():
                if not line.strip():
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                (network if name.endswith(".network") else events).append(rec)

    starts, actions, errors = {}, [], []
    for e in events:
        kind = e.get("type")
        if kind == "before":
            starts[e.get("callId")] = e
        elif kind == "after":
            b = starts.get(e.get("callId"), {})
            if str(b.get("method", "")).startswith("__"):
                continue
            p = b.get("params", {})
            target = p.get("selector") or p.get("url") or p.get("value") or ""
            ms = (e.get("endTime", 0) - b.get("startTime", 0))
            err = (e.get("error") or {}).get("message", "")
            actions.append((b.get("method", "?"), str(target)[:90], ms, err))
        elif kind == "console" and e.get("messageType") == "error":
            errors.append(f"console: {e.get('text', '')[:200]}")
        elif kind == "event" and e.get("method") == "pageError":
            err = e.get("params", {}).get("error", {})
            msg = err.get("error", {}).get("message") or err.get("value") or str(err)
            errors.append(f"pageerror: {str(msg)[:200]}")

    failed_requests = []
    for rec in network:
        s = rec.get("snapshot", {})
        status = s.get("response", {}).get("status", 0)
        failure = s.get("response", {}).get("_failureText")
        if failure or status >= 400 or status == -1:
            failed_requests.append(f"{failure or status} {s.get('request', {}).get('url', '')[:160]}")

    shown = actions if show_all else actions[-15:]
    if len(shown) < len(actions):
        print(f"... {len(actions) - len(shown)} earlier actions (use --all)")
    for method, target, ms, err in shown:
        mark = "FAIL" if err else " ok "
        print(f"{mark} {method:<16} {ms:>7.0f} ms  {target}")
        if err:
            print(f"     error: {err.splitlines()[0][:200]}")
    print(f"\nactions: {len(actions)}, failed: {sum(1 for a in actions if a[3])}")
    for line in errors[:10]:
        print(line)
    for line in failed_requests[:10]:
        print(f"request: {line}")
    return 1 if any(a[3] for a in actions) else 0


if __name__ == "__main__":
    sys.exit(main())
