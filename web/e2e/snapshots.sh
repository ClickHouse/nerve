#!/bin/sh
# Runs the UI snapshot tests in the Playwright image, as CI does. `--update-snapshots` rewrites the baselines.
set -eu
cd "$(dirname "$0")/../.."
version=$(node -p "require('./web/package.json').devDependencies['@playwright/test']")
docker run --rm -e CI -e PYTHONDONTWRITEBYTECODE=1 -v "$PWD:/work" -v /work/web/node_modules -v /work/.venv -w /work/web \
  "mcr.microsoft.com/playwright:v$version-noble" bash -euo pipefail -c '
    curl -LsSf https://astral.sh/uv/0.12.0/install.sh | UV_NO_MODIFY_PATH=1 sh >/dev/null
    "$HOME/.local/bin/uv" sync --locked --python 3.13 --project .. --quiet
    npm ci --strict-peer-deps --no-audit --no-fund >/dev/null
    npm run build >/dev/null
    npx playwright test "$@"' sh "$@"
