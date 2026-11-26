"""Step 2 of the PPTX export (slide-deck skill): builds deck.pptx from
pptx/items.json and pptx/img/ (made by pptx-export.mjs).

Copy into the deck folder. Needs a venv there (see the skill):
    .venv/bin/python pptx-build.py

Text stays editable text. The biggest headline (class h) goes into the title
placeholder. Text colour, background, accent and fonts go into the theme, so a
company template can replace them; only colours other than the theme ink are
set on the text itself. Each click step becomes an Appear or Disappear
animation, and the notes list the click builds as a backup.
"""
import glob
import json
import os
import re

from lxml import etree
from PIL import Image
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.dml import MSO_LINE_DASH_STYLE
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, MSO_AUTO_SIZE, MSO_UNDERLINE, PP_ALIGN
from pptx.opc.constants import RELATIONSHIP_TYPE as RT
from pptx.util import Emu, Pt

DIR = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(DIR, 'deck.pptx')
MAX_PX = 2000  # Google Slides resamples larger pictures badly on import
FONT = None       # theme body font; None: the first font of the deck's body font stack
HEAD_FONT = None  # theme headline font; None: the first font of the headline's stack
# Ascent plus descent of a font in em: (hhea.ascent - hhea.descent) / unitsPerEm.
# The builder reads it from the font file in fonts/ when one matches; else from this table.
# A wrong value moves single lines of text up or down in the PPTX.
# Measured with fontTools; Arial and Times New Roman share the metrics of their Liberation twins.
NAT = {'Inter': 1.210, 'Jost': 1.445, 'Lato': 1.200, 'Source Serif 4': 1.371, 'Liberation Sans': 1.117,
       'Arial': 1.117, 'Liberation Serif': 1.107, 'Times New Roman': 1.107, 'DejaVu Sans': 1.164,
       'DejaVu Serif': 1.164}
NAT_DEFAULT = 1.2

data = json.load(open(os.path.join(DIR, 'pptx', 'items.json'), encoding='utf-8'))
CW, CH = data['canvas']['w'], data['canvas']['h']
PX = 12192000 / CW  # EMU per CSS px: the canvas width becomes 13.333 in (16:9 widescreen width)
PT = PX / 12700     # points per CSS px
theme_in = data['theme']
FONT = FONT or theme_in['font']
HEAD_FONT = HEAD_FONT or theme_in['headFont']


def measured_nat(family):
    """Returns (nat, file) from a font file in fonts/ whose name matches family, else (None, None)."""
    try:
        from fontTools.ttLib import TTFont
    except ImportError:
        return None, None
    want = family.lower().replace(' ', '')
    for f in sorted(glob.glob(os.path.join(DIR, 'fonts', '*'))):
        if not f.lower().endswith(('.woff2', '.woff', '.ttf', '.otf')):
            continue
        try:
            t = TTFont(f, lazy=True)
            names = {n.toUnicode().lower().replace(' ', '') for n in t['name'].names if n.nameID in (1, 16)}
        except Exception as e:  # noqa: BLE001 (a woff2 needs the brotli package)
            print('note: cannot read', os.path.basename(f), '-', e)
            continue
        if want in names or os.path.basename(f).lower().replace('-', '').startswith(want):
            h = t['hhea']
            return (h.ascent - h.descent) / t['head'].unitsPerEm, os.path.basename(f)
    return None, None


NATS = {}
for fam in sorted({FONT, HEAD_FONT}):
    v, src = measured_nat(fam)
    if v is None and fam in NAT:
        v, src = NAT[fam], 'table'
    if v is None:
        v, src = NAT_DEFAULT, 'default: not measured, see the skill'
    NATS[fam] = v
    print(f'font {fam}: NAT {v:.3f} ({src})')


def nat(family):
    return NATS.get(family) or NAT.get(family) or NAT_DEFAULT


def fit(path):
    """Returns a copy of the picture no larger than MAX_PX on the long edge."""
    im = Image.open(path)
    if max(im.size) <= MAX_PX:
        return path
    k = MAX_PX / max(im.size)
    out = path.replace(os.sep + 'img' + os.sep, os.sep + 'img-fit' + os.sep)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    im.resize((round(im.width * k), round(im.height * k)), Image.LANCZOS).save(out, optimize=True)
    return out


def rgb(css):
    m = re.match(r'rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*([\d.]+))?', css or '')
    if not m:
        return None
    return tuple(int(m.group(i)) for i in (1, 2, 3)), float(m.group(4) or 1)


def hexof(css, default):
    c = rgb(css)
    return '%02X%02X%02X' % c[0] if c else default


def emu(v):
    return Emu(int(round(v * PX)))


prs = Presentation()
prs.slide_width, prs.slide_height = emu(CW), emu(CH)

# Theme: the deck's ink, background, accent and fonts. A template replaces all of this.
INK = rgb(theme_in['ink'])[0] if rgb(theme_in['ink']) else (0, 0, 0)
BG = rgb(theme_in['bg'])[0] if rgb(theme_in['bg']) else (255, 255, 255)
theme = prs.slide_master.part.part_related_by(RT.THEME)
x = theme.blob.decode()
x = re.sub(r'<a:dk1>.*?</a:dk1>', '<a:dk1><a:srgbClr val="%s"/></a:dk1>' % hexof(theme_in['ink'], '000000'), x, flags=re.S)
x = re.sub(r'<a:lt1>.*?</a:lt1>', '<a:lt1><a:srgbClr val="%s"/></a:lt1>' % hexof(theme_in['bg'], 'FFFFFF'), x, flags=re.S)
x = re.sub(r'<a:accent1>.*?</a:accent1>', '<a:accent1><a:srgbClr val="%s"/></a:accent1>' % hexof(theme_in['accent'], '2456C4'), x, flags=re.S)
x = re.sub(r'(<a:majorFont>\s*<a:latin typeface=")[^"]*"', lambda m: m.group(1) + HEAD_FONT + '"', x)
x = re.sub(r'(<a:minorFont>\s*<a:latin typeface=")[^"]*"', lambda m: m.group(1) + FONT + '"', x)
theme._blob = x.encode()

L_TITLE, L_BLANK = prs.slide_layouts[5], prs.slide_layouts[6]
ALIGN = {'center': PP_ALIGN.CENTER, 'right': PP_ALIGN.RIGHT, 'end': PP_ALIGN.RIGHT}


def style_shape(shp, deco, box):
    c = rgb(deco.get('bg')) if deco.get('hasBg') else None
    if c and c[1] > 0.05:
        shp.fill.solid()
        shp.fill.fore_color.rgb = RGBColor(*c[0])
    else:
        shp.fill.background()
    if deco.get('hasBorder'):
        bc = rgb(deco['bc'])
        shp.line.width = Pt(deco['bw'] * PT)
        if bc:
            shp.line.color.rgb = RGBColor(*bc[0])
        if deco.get('bs') in ('dashed', 'dotted'):
            shp.line.dash_style = MSO_LINE_DASH_STYLE.DASH
    else:
        shp.line.fill.background()
    if deco.get('radius') and shp.auto_shape_type == MSO_SHAPE.ROUNDED_RECTANGLE:
        shp.adjustments[0] = min(0.5, deco['radius'] / max(1, min(box[2], box[3])))
    shp.shadow.inherit = False


def add_deco(slide, deco, box):
    kind = MSO_SHAPE.ROUNDED_RECTANGLE if deco.get('radius') else MSO_SHAPE.RECTANGLE
    shp = slide.shapes.add_shape(kind, emu(box[0]), emu(box[1]), emu(box[2]), emu(box[3]))
    style_shape(shp, deco, box)
    return shp


def add_bars(slide, deco, box):
    """A border on one side only (a rule or a marker) becomes a thin filled rectangle."""
    made = []
    x, y, w, h = box
    for b in deco.get('bars', []):
        c = rgb(b['c'])
        if not c or c[1] < 0.05:
            continue
        r = {'top': (x, y, w, b['w']), 'bottom': (x, y + h - b['w'], w, b['w']),
             'left': (x, y, b['w'], h), 'right': (x + w - b['w'], y, b['w'], h)}[b['side']]
        shp = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, emu(r[0]), emu(r[1]), emu(r[2]), emu(r[3]))
        shp.fill.solid()
        shp.fill.fore_color.rgb = RGBColor(*c[0])
        shp.line.fill.background()
        shp.shadow.inherit = False
        made.append(shp)
    return made


def fill_text(tf, it, multi, base_font):
    runs = [dict(r) for r in it['runs']]
    runs[0]['text'] = runs[0]['text'].lstrip()
    runs[-1]['text'] = runs[-1]['text'].rstrip()
    paras = [[]]
    for r in runs:
        for k, piece in enumerate(r['text'].split('\n')):
            if k:
                paras.append([])
            if piece:
                paras[-1].append((piece, r))
    tf.clear()
    for i, pr in enumerate(paras):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.alignment = ALIGN.get(it['align'], PP_ALIGN.LEFT)
        if multi and it['lh'].endswith('px'):
            p.line_spacing = Pt(float(it['lh'][:-2]) * PT)
        if pr:
            pr[0] = (pr[0][0].lstrip(), pr[0][1])
            pr[-1] = (pr[-1][0].rstrip(), pr[-1][1])
        for piece, r in pr:
            run = p.add_run()
            run.text = piece
            f = run.font
            f.size = Pt(r['size'] * PT)
            f.bold = r['bold']
            f.italic = r['italic'] or None
            if r.get('font') and r['font'] != base_font:
                f.name = r['font']
            if r.get('ls'):  # letter spacing, in hundredths of a point
                f._rPr.set('spc', str(int(round(r['ls'] * PT * 100))))
            if r.get('u'):
                f.underline = MSO_UNDERLINE.DOTTED_LINE if r['u'] == 'dotted' else True
            c = rgb(r['color'])
            if c and c[0] != INK:
                f.color.rgb = RGBColor(*c[0])


def add_text(slide, it, target=None):
    d = it['deco']
    filled = (d.get('hasBg') or d.get('hasBorder')) and not it['kids']
    made = add_bars(slide, d, it['ebox'])
    if (d.get('hasBg') or d.get('hasBorder')) and it['kids']:
        made.append(add_deco(slide, d, it['ebox']))  # background behind, text placed separately
    base_font = HEAD_FONT if target is not None else FONT
    if filled:
        x, y, w, h = it['ebox']
        shp = add_deco(slide, d, it['ebox'])
        tf = shp.text_frame
        pl, pt, pr, pb = d['pad']
        tf.margin_left, tf.margin_top, tf.margin_right, tf.margin_bottom = emu(pl), emu(pt), emu(pr), emu(pb)
        tf.vertical_anchor = MSO_ANCHOR.MIDDLE
        tf.word_wrap = True
        if it['lines'] <= 1:  # Slides wraps text, so pills get room to spare
            shp.width = emu(w * 1.12)
        multi = it['lines'] > 1
    else:
        x, y, w, h = it['box']
        single = it['lines'] <= 1 and not any('\n' in r['text'].strip() for r in it['runs'])
        # Slides always wraps text, so single lines get room to spare
        slack = (0.25 * w + 24) if single else (0.08 * w + 12)
        if it['align'] == 'center':
            x -= slack / 2
        elif it['align'] in ('right', 'end'):
            x -= slack
        w += slack
        x = max(0, x)
        w = min(w, CW - x)
        # the measured box is the font's full height, so it lines up with natural spacing
        h = max(h, it['size'] * nat(it.get('font')) * (1 if single else it['lines'] * 0.75))
        multi = not single
        if target is not None:
            shp = target
            shp.left, shp.top, shp.width, shp.height = emu(x), emu(y), emu(w), emu(h)
        else:
            shp = slide.shapes.add_textbox(emu(x), emu(y), emu(w), emu(h))
        tf = shp.text_frame
        tf.margin_left = tf.margin_right = tf.margin_top = tf.margin_bottom = 0
        tf.vertical_anchor = MSO_ANCHOR.TOP
        tf.word_wrap = not single
    tf.auto_size = MSO_AUTO_SIZE.NONE
    fill_text(tf, it, multi, base_font)
    return made + [shp]


P_NS = 'http://schemas.openxmlformats.org/presentationml/2006/main'


def add_timing(slide, clicks, pics):
    """Writes click animations: clicks = {click: [(shape_id, 'in'|'out'), ...]}.
    'in' is the Appear entrance effect, 'out' the Disappear exit effect."""
    ids = iter(range(3, 100000))
    grp = {}
    pars = []
    for c in sorted(clicks):
        effs = []
        for j, (spid, kind) in enumerate(clicks[c]):
            g = grp[spid] = grp.get(spid, -1) + 1
            effs.append(
                '<p:par><p:cTn id="{a}" presetID="1" presetClass="{cls}" presetSubtype="0" fill="hold" grpId="{g}" nodeType="{node}">'
                '<p:stCondLst><p:cond delay="0"/></p:stCondLst><p:childTnLst><p:set><p:cBhvr>'
                '<p:cTn id="{b}" dur="1" fill="hold"><p:stCondLst><p:cond delay="0"/></p:stCondLst></p:cTn>'
                '<p:tgtEl><p:spTgt spid="{spid}"/></p:tgtEl><p:attrNameLst><p:attrName>style.visibility</p:attrName></p:attrNameLst>'
                '</p:cBhvr><p:to><p:strVal val="{val}"/></p:to></p:set></p:childTnLst></p:cTn></p:par>'
                .format(a='{a}', b='{b}', cls='entr' if kind == 'in' else 'exit', g=g,
                        node='clickEffect' if j == 0 else 'withEffect', spid=spid,
                        val='visible' if kind == 'in' else 'hidden'))
        outer, inner = next(ids), next(ids)
        body = ''
        for e in effs:
            body += e.replace('{a}', str(next(ids)), 1).replace('{b}', str(next(ids)), 1)
        pars.append('<p:par><p:cTn id="%d" fill="hold"><p:stCondLst><p:cond delay="indefinite"/></p:stCondLst><p:childTnLst>'
                    '<p:par><p:cTn id="%d" fill="hold"><p:stCondLst><p:cond delay="0"/></p:stCondLst><p:childTnLst>%s'
                    '</p:childTnLst></p:cTn></p:par></p:childTnLst></p:cTn></p:par>' % (outer, inner, body))
    blds = ''.join('<p:bldP spid="%d" grpId="%d" animBg="1"/>' % (spid, g)
                   for spid, n in grp.items() if spid not in pics for g in range(n + 1))
    xml = ('<p:timing xmlns:p="%s"><p:tnLst><p:par><p:cTn id="1" dur="indefinite" restart="never" nodeType="tmRoot"><p:childTnLst>'
           '<p:seq concurrent="1" nextAc="seek"><p:cTn id="2" dur="indefinite" nodeType="mainSeq"><p:childTnLst>%s</p:childTnLst></p:cTn>'
           '<p:prevCondLst><p:cond evt="onPrev" delay="0"><p:tgtEl><p:sldTgt/></p:tgtEl></p:cond></p:prevCondLst>'
           '<p:nextCondLst><p:cond evt="onNext" delay="0"><p:tgtEl><p:sldTgt/></p:tgtEl></p:cond></p:nextCondLst>'
           '</p:seq></p:childTnLst></p:cTn></p:par></p:tnLst>%s</p:timing>'
           % (P_NS, ''.join(pars), '<p:bldLst>%s</p:bldLst>' % blds if blds else ''))
    slide._element.find('{%s}clrMapOvr' % P_NS).addnext(etree.fromstring(xml))


for s in data['slides']:
    texts = [it for it in s['items'] if it['kind'] == 'text']
    heads = [it for it in texts if it['headline']]
    head = max(heads, key=lambda it: (it['size'], it['first'] == 0)) if heads else None
    slide = prs.slides.add_slide(L_TITLE if head else L_BLANK)
    bg = rgb(s.get('bg'))
    if bg and bg[1] > 0.05 and bg[0] != BG:  # a slide with its own background colour
        slide.background.fill.solid()
        slide.background.fill.fore_color.rgb = RGBColor(*bg[0])
    clicks, pics = {}, set()
    for it in s['items']:
        later = []
        if it['kind'] == 'pic':
            made = []
            for k, im in enumerate(it['imgs']):
                b = im['box']
                pic = slide.shapes.add_picture(fit(os.path.join(DIR, 'pptx', im['file'])), emu(b[0]), emu(b[1]), emu(b[2]), emu(b[3]))
                pics.add(pic.shape_id)
                if k == 0:
                    made = [pic]
                else:  # the next state of the mockup covers the one before it
                    later.append(pic)
                    clicks.setdefault(im['state'], []).append((pic.shape_id, 'in'))
        elif it['kind'] == 'shape':
            made = add_bars(slide, it['deco'], it['box'])
            if it['deco'].get('hasBg') or it['deco'].get('hasBorder'):
                made.append(add_deco(slide, it['deco'], it['box']))
        else:
            made = add_text(slide, it, target=slide.shapes.title if it is head else None)
        if it['first'] > 0:
            for m in made:
                clicks.setdefault(it['first'], []).append((m.shape_id, 'in'))
        if it['last'] < s['k']:
            for m in made + later:
                clicks.setdefault(it['last'] + 1, []).append((m.shape_id, 'out'))
    if clicks:
        add_timing(slide, clicks, pics)
    notes = list(s['notes'])
    if s['builds']:
        notes.append('CLICK BUILDS (in this file as Appear / Disappear animations; a backup list in case the import drops them):')
        for k in sorted(s['builds'], key=int):
            notes.append('  Click %s: %s' % (k, '; '.join(s['builds'][k])))
    if s.get('media'):
        notes.append('MEDIA: this slide plays sound or video in the HTML deck. Insert it again here.')
    if s.get('bgImage'):
        notes.append('BACKGROUND: the HTML slide has a background picture or gradient. It is not in this file.')
    notes.append('HTML slide %d: %s' % (s['n'], s['title']))
    slide.notes_slide.notes_text_frame.text = '\n'.join(notes)

prs.save(OUT)
print('saved', os.path.basename(OUT), len(data['slides']), 'slides, canvas', '%dx%d' % (CW, CH))
