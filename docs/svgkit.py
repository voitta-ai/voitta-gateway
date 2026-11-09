"""Tiny SVG drawing helper used by diagrams.py (no dependencies).

Coordinates are absolute pixels. Arrowheads are drawn as explicit polygons
(not SVG markers) so every renderer — browsers, cairosvg, rsvg — agrees.
"""
from __future__ import annotations

import math
from html import escape
from pathlib import Path

FONT = "Helvetica, Arial, sans-serif"
MONO = "Menlo, Consolas, monospace"

# kind -> (fill, stroke)
PALETTE = {
    "config":   ("#f4f4f5", "#52525b"),
    "platform": ("#ffffff", "#27272a"),
    "msg":      ("#eaf1fe", "#1d4ed8"),
    "voice":    ("#e7f6ee", "#047857"),
    "core":     ("#fdf2f8", "#be185d"),
    "api":      ("#fff6e5", "#b45309"),
    "consumer": ("#f3ecfe", "#6d28d9"),
    "neutral":  ("#ffffff", "#71717a"),
    "later":    ("#fafafa", "#a1a1aa"),
    "host":     ("#fbfbfc", "#3f3f46"),
}
# Status badges: what has been built and how it was tested.
STATUS = {
    "live":    ("#15803d", "LIVE",    "built; tested end to end on real Signal calls (.212)"),
    "tested":  ("#2563eb", "TESTED",  "built; tested end to end against simulated platforms"),
    "partial": ("#b45309", "PARTIAL", "built in part (see text)"),
    "planned": ("#9ca3af", "PLANNED", "designed, not built"),
}
INK = "#18181b"
MUTED = "#52525b"
WIRE = "#3f3f46"


# Glyphs missing from common fonts (cairo/rsvg fall back to tofu boxes).
_ASCII = {"→": "->", "⇄": "<->", "⇅": "<->", "≥": ">=", "≤": "<=", "…": "..."}


def _safe(s: str) -> str:
    for k, v in _ASCII.items():
        s = s.replace(k, v)
    return s


def _w(s: str, size: float) -> float:
    """Rough text width estimate (Helvetica ~0.56em average)."""
    return len(s) * size * 0.56


class SVG:
    def __init__(self, w: int, h: int, title: str):
        self.w, self.h, self.title = w, h, title
        self.items: list[str] = []

    # ---------- primitives ----------
    def rect(self, x, y, w, h, kind="neutral", rx=8, dashed=False, sw=1.5,
             fill=None, stroke=None):
        f, s = PALETTE[kind]
        dash = ' stroke-dasharray="6 4"' if dashed else ""
        self.items.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" '
            f'fill="{fill or f}" stroke="{stroke or s}" stroke-width="{sw}"{dash}/>'
        )

    def text(self, x, y, s, size=13, weight="normal", anchor="middle",
             color=INK, italic=False, mono=False, bg=False):
        s = _safe(s)
        if bg:
            tw = _w(s, size) + 8
            bx = {"middle": x - tw / 2, "start": x - 4, "end": x - tw + 4}[anchor]
            self.items.append(
                f'<rect x="{bx:.1f}" y="{y - size + 1:.1f}" width="{tw:.1f}" '
                f'height="{size + 5}" fill="#ffffff" fill-opacity="0.92"/>'
            )
        style = ' font-style="italic"' if italic else ""
        fam = MONO if mono else FONT
        self.items.append(
            f'<text x="{x}" y="{y}" font-family="{fam}" font-size="{size}" '
            f'font-weight="{weight}" text-anchor="{anchor}" fill="{color}"{style}>'
            f"{escape(s)}</text>"
        )

    def line(self, pts, color=WIRE, sw=1.6, dashed=False):
        d = " ".join(f"{x},{y}" for x, y in pts)
        dash = ' stroke-dasharray="6 4"' if dashed else ""
        self.items.append(
            f'<polyline points="{d}" fill="none" stroke="{color}" '
            f'stroke-width="{sw}" stroke-linejoin="round"{dash}/>'
        )

    def _head(self, p_from, p_to, color, size=9):
        (x1, y1), (x2, y2) = p_from, p_to
        a = math.atan2(y2 - y1, x2 - x1)
        l = (x2 - size * math.cos(a - 0.42), y2 - size * math.sin(a - 0.42))
        r = (x2 - size * math.cos(a + 0.42), y2 - size * math.sin(a + 0.42))
        self.items.append(
            f'<polygon points="{x2},{y2} {l[0]:.1f},{l[1]:.1f} {r[0]:.1f},{r[1]:.1f}" '
            f'fill="{color}"/>'
        )

    def arrow(self, pts, color=WIRE, both=False, dashed=False, sw=1.6,
              label=None, label_at=None, label_size=11, label_dy=-6,
              label_color=None):
        self.line(pts, color, sw, dashed)
        self._head(pts[-2], pts[-1], color)
        if both:
            self._head(pts[1], pts[0], color)
        if label:
            if label_at is None:
                (x1, y1), (x2, y2) = pts[0], pts[1]
                label_at = ((x1 + x2) / 2, (y1 + y2) / 2 + label_dy)
            for i, ln in enumerate(label.split("\n")):
                self.text(label_at[0], label_at[1] + i * (label_size + 3), ln,
                          size=label_size, color=label_color or MUTED, bg=True)

    # ---------- status ----------
    def badge(self, x_right, y, status):
        """A pill whose right edge is x_right, centred on y."""
        color, label, _ = STATUS[status]
        w = len(label) * 6.4 + 12
        self.items.append(
            f'<rect x="{x_right - w:.1f}" y="{y - 8}" width="{w:.1f}" height="16" rx="8" fill="{color}"/>')
        self.items.append(
            f'<text x="{x_right - w / 2:.1f}" y="{y + 4}" font-family="{FONT}" font-size="10" '
            f'font-weight="bold" text-anchor="middle" fill="#ffffff">{label}</text>')

    def legend(self, x, y, statuses=("live", "tested", "partial", "planned")):
        for st in statuses:
            color, label, desc = STATUS[st]
            w = len(label) * 6.4 + 12
            self.badge(x + w, y, st)
            self.text(x + w + 8, y + 4, desc, size=11.5, anchor="start", color=MUTED)
            x += w + 8 + len(desc) * 11.5 * 0.52 + 16

    # ---------- composites ----------
    def box(self, x, y, w, h, title, lines=(), kind="neutral", dashed=False,
            title_size=14, line_size=11.5, mono_lines=False, align="middle", status=None):
        self.rect(x, y, w, h, kind, dashed=dashed)
        if status:
            self.badge(x + w - 8, y, status)              # sits on the top border
        stroke = PALETTE[kind][1]
        n = len(lines)
        block = title_size + n * (line_size + 4)
        top = y + (h - block) / 2 + title_size - 2
        tx = x + w / 2 if align == "middle" else x + 12
        self.text(tx, top, title, size=title_size, weight="bold",
                  color=stroke if kind not in ("platform", "neutral") else INK,
                  anchor=align)
        for i, ln in enumerate(lines):
            self.text(tx, top + (i + 1) * (line_size + 4) + 1, ln,
                      size=line_size, color=MUTED, mono=mono_lines, anchor=align)

    def group(self, x, y, w, h, label, kind, sub=None, dashed=False,
              fill_opacity_hex=None, status=None):
        f, s = PALETTE[kind]
        self.rect(x, y, w, h, kind, rx=12, dashed=dashed, sw=1.8)
        if status:
            self.badge(x + w - 12, y, status)
        self.text(x + 16, y + 22, label, size=15, weight="bold", anchor="start",
                  color=s)
        if sub:
            self.text(x + 16, y + 39, sub, size=11.5, anchor="start", color=MUTED,
                      italic=True)

    # ---------- output ----------
    def render(self) -> str:
        body = "\n  ".join(self.items)
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" '
            f'height="{self.h}" viewBox="0 0 {self.w} {self.h}">\n'
            f"  <title>{escape(self.title)}</title>\n"
            f'  <rect width="100%" height="100%" fill="#ffffff"/>\n'
            f"  {body}\n</svg>\n"
        )

    def save(self, out_dir: Path, name: str, png_scale: float = 2.0) -> Path:
        out_dir.mkdir(parents=True, exist_ok=True)
        svg_path = out_dir / f"{name}.svg"
        svg_path.write_text(self.render(), encoding="utf-8")
        try:
            import cairosvg  # optional: PNG copies for embedding in DOCX

            cairosvg.svg2png(url=str(svg_path), write_to=str(out_dir / f"{name}.png"),
                             scale=png_scale)
        except ImportError:
            pass
        return svg_path


class Sequence:
    """Minimal sequence-diagram layout on top of SVG."""

    def __init__(self, svg: SVG, participants: list[tuple[str, str, str]],
                 x0=110, x1=None, top=30, head_h=54, head_w=190):
        # participants: (key, title, kind)
        self.s = svg
        x1 = x1 or svg.w - 110
        n = len(participants)
        step = (x1 - x0) / (n - 1)
        self.x = {k: x0 + i * step for i, (k, _, _) in enumerate(participants)}
        self.parts = participants
        self.top, self.head_h, self.head_w = top, head_h, head_w
        self.y = top + head_h + 30

    def heads(self, bottom):
        self.s.h = int(bottom + 30)  # fit canvas to content
        lifelines = []
        for k, title, kind in self.parts:
            cx = self.x[k]
            lifelines.append(
                f'<polyline points="{cx},{self.top + self.head_h} {cx},{bottom}" '
                f'fill="none" stroke="#a1a1aa" stroke-width="1.2" stroke-dasharray="6 4"/>')
            lines = title.split("\n")
            self.s.box(cx - self.head_w / 2, self.top, self.head_w, self.head_h,
                       lines[0], lines[1:], kind=kind, title_size=13, line_size=11)
        self.s.items[0:0] = lifelines  # behind messages and notes

    def msg(self, a, b, label, dashed=False, both=False, color=WIRE, gap=38):
        xa, xb = self.x[a], self.x[b]
        y = self.y
        self.s.arrow([(xa, y), (xb, y)], color=color, dashed=dashed, both=both)
        self.s.text((xa + xb) / 2, y - 7, label, size=11.5, color=INK, bg=True)
        self.y += gap

    def note(self, a, b, label, kind="api", h=30, gap=44, pad=70):
        xa, xb = sorted((self.x[a], self.x[b]))
        f, s = PALETTE[kind]
        need = _w(label, 11.5) * 1.12 + 30  # bold text is wider
        if xb - xa + 2 * pad < need:
            pad = (need - (xb - xa)) / 2
        self.s.rect(xa - pad, self.y - 18, xb - xa + 2 * pad, h, kind, rx=6, sw=1)
        self.s.text((xa + xb) / 2, self.y + 1, label, size=11.5, color=s,
                    weight="bold")
        self.y += gap
