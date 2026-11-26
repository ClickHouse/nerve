#!/usr/bin/env bash
# Pictures of deck.pptx as LibreOffice draws it (slide-deck skill). An optional check.
# Run in the deck folder, after pptx-build.py:  bash <skill>/scripts/pptx-preview.sh [dpi, default 50]
# Needs Docker. Builds the image slide-deck-libreoffice once (about 670 MB on disk),
# then converts in a throwaway container with no network.
# Output: pptx-preview/deck.pdf, p-N.png, sheet.png (all pages), and the clicks per slide
# as LibreOffice reads them, next to the clicks of the HTML deck.
# Remove the image when the deck is done: docker rmi slide-deck-libreoffice
set -euo pipefail
IMG=slide-deck-libreoffice
DPI=${1:-50}
D=$(pwd)
[ -f deck.pptx ] || { echo "no deck.pptx in $D: run pptx-export.mjs and pptx-build.py first"; exit 2; }
if ! docker image inspect "$IMG" >/dev/null 2>&1; then
  echo "building the image $IMG (once)"
  docker build -q -t "$IMG" - <<'EOF'
FROM ubuntu:24.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends libreoffice-impress poppler-utils fonts-dejavu-core fontconfig && rm -rf /var/lib/apt/lists/*
EOF
fi
rm -rf pptx-preview
mkdir -p pptx-preview/fonts
cp deck.pptx pptx-preview/
# LibreOffice cannot read woff2: the deck's fonts go in as TrueType (fontTools in the deck venv)
if compgen -G "fonts/*.woff2" >/dev/null && [ -x .venv/bin/python ]; then
  .venv/bin/python -I - <<'EOF'
import glob, os
from fontTools.ttLib import TTFont
for f in glob.glob('fonts/*.woff2'):
    t = TTFont(f); t.flavor = None
    t.save('pptx-preview/fonts/' + os.path.basename(f)[:-6] + '.ttf')
EOF
fi
docker run --rm --network none --user "$(id -u):$(id -g)" -e HOME=/tmp -e DPI="$DPI" \
  -v "$D/pptx-preview:/d" -w /d "$IMG" bash -c '
  mkdir -p /tmp/.fonts && cp fonts/*.ttf /tmp/.fonts/ 2>/dev/null || true
  fc-cache -f >/dev/null 2>&1
  soffice --headless --convert-to pdf deck.pptx >/dev/null 2>&1
  soffice --headless --convert-to odp deck.pptx >/dev/null 2>&1 || true
  pdftoppm -r "$DPI" -png deck.pdf p'
ls pptx-preview/p-*.png >/dev/null 2>&1 || { echo "no pages: the conversion failed"; exit 1; }
# clicks per slide as LibreOffice reads the animations, next to the clicks of the HTML deck
python3 -I - <<'EOF'
import json, os, re, zipfile
if not os.path.exists('pptx-preview/deck.odp'):
    raise SystemExit('no deck.odp: click count not checked')
x = zipfile.ZipFile('pptx-preview/deck.odp').read('content.xml').decode()
got = [len(re.findall(r'presentation:node-type="on-click"', p)) for p in re.findall(r'<draw:page .*?</draw:page>', x, re.S)]
want = [s['k'] for s in json.load(open('pptx/items.json'))['slides']] if os.path.exists('pptx/items.json') else []
print('clicks in LibreOffice', got)
print('clicks in the HTML   ', want, 'same' if got == want else 'DIFFERENT: look at those slides')
EOF
if [ -x .venv/bin/python ]; then
  .venv/bin/python -I - <<'EOF'
import glob, re
from PIL import Image
files = sorted(glob.glob('pptx-preview/p-*.png'), key=lambda f: int(re.findall(r'(\d+)\.png$', f)[0]))
ims = [Image.open(f) for f in files]
w, h = ims[0].size
cols = 4
rows = (len(ims) + cols - 1) // cols
sheet = Image.new('RGB', (cols * (w + 8) + 8, rows * (h + 8) + 8), (136, 136, 136))
for i, im in enumerate(ims):
    sheet.paste(im, (8 + (i % cols) * (w + 8), 8 + (i // cols) * (h + 8)))
sheet.save('pptx-preview/sheet.png')
print('pages', len(ims), 'sheet pptx-preview/sheet.png')
EOF
else
  ls pptx-preview/p-*.png
fi
