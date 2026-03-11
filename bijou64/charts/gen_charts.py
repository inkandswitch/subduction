#!/usr/bin/env python3
"""Generate SVG line charts for the bijou64 benchmark shootout.

Each chart has labelled lines, a proper legend, gridlines, and axis labels.
Output goes to bijou64/charts/*.svg.

Run: nix run nixpkgs#python3 -- bijou64/gen_charts.py
"""

import math
import os

# ---------------------------------------------------------------------------
# Chart data
# ---------------------------------------------------------------------------

DISTRIBUTIONS = ["tiny", "small", "medium", "large", "tier", "uniform"]

CHARTS = [
    {
        "filename": "encode_vec.svg",
        "title": "Encode (Vec) — median µs / 4096 values",
        "series": [
            ("bijou64", [2.26, 11.41, 19.29, 13.11, 16.26, 13.15]),
            ("varu64",  [10.96, 20.00, 26.89, 28.19, 29.47, 28.10]),
            ("vu64",    [20.69, 22.96, 22.76, 11.30, 18.52, 11.21]),
            ("vu128",   [16.23, 20.17, 21.95, 11.70, 19.15, 11.84]),
            ("leb128",  [4.21, 7.16, 13.55, 33.25, 12.03, 34.17]),
        ],
    },
    {
        "filename": "encode_array.svg",
        "title": "Encode Array (no alloc) — median µs / 4096 values",
        "series": [
            ("bijou64", [1.27, 2.41, 2.59, 2.75, 2.58, 2.54]),
            ("varu64",  [4.87, 8.67, 12.38, 19.88, 16.48, 19.92]),
            ("vu64",    [1.62, 1.63, 1.65, 1.64, 1.65, 1.65]),
            ("vu128",   [2.87, 3.51, 3.52, 3.51, 3.41, 3.53]),
        ],
    },
    {
        "filename": "decode.svg",
        "title": "Decode — median µs / 4096 values",
        "series": [
            ("bijou64", [3.93, 9.36, 8.77, 10.05, 11.59, 10.34]),
            ("varu64",  [6.62, 10.99, 16.24, 22.27, 19.21, 23.86]),
            ("vu64",    [14.40, 15.43, 15.37, 8.80, 12.27, 9.30]),
            ("vu128",   [22.76, 15.18, 10.70, 8.67, 10.78, 9.22]),
            ("leb128",  [12.57, 15.24, 16.09, 35.86, 15.39, 35.52]),
        ],
    },
    {
        "filename": "stream_decode.svg",
        "title": "Stream Decode — median µs / 4096 values",
        "series": [
            ("bijou64", [3.98, 9.34, 9.34, 10.31, 19.76, 9.81]),
            ("varu64",  [9.77, 20.82, 18.12, 23.29, 23.28, 22.48]),
            ("vu64",    [17.51, 18.84, 16.03, 8.68, 14.14, 8.57]),
            ("leb128",  [7.20, 13.02, 17.02, 35.76, 14.15, 34.89]),
        ],
    },
    {
        "filename": "round_trip.svg",
        "title": "Round Trip — median µs / 4096 values",
        "series": [
            ("bijou64", [4.96, 20.54, 27.87, 22.92, 27.50, 23.57]),
            ("varu64",  [10.49, 17.76, 28.07, 41.98, 34.70, 43.46]),
            ("vu64",    [22.94, 26.32, 22.52, 11.47, 17.43, 12.07]),
            ("vu128",   [4.73, 7.06, 2.91, 2.61, 4.23, 2.77]),
            ("leb128",  [13.22, 22.90, 26.93, 57.63, 25.89, 59.84]),
        ],
    },
]

# Distinct, accessible palette (colour-blind friendly, high contrast on white)
COLOURS = {
    "bijou64": "#2563eb",  # blue
    "varu64":  "#d97706",  # amber/orange
    "vu64":    "#dc2626",  # red
    "vu128":   "#059669",  # emerald/teal
    "leb128":  "#7c3aed",  # violet
}

# ---------------------------------------------------------------------------
# SVG generation
# ---------------------------------------------------------------------------

# Layout constants
MARGIN_LEFT = 58
MARGIN_RIGHT = 110  # room for legend
MARGIN_TOP = 40
MARGIN_BOTTOM = 50
PLOT_W = 460
PLOT_H = 260
TOTAL_W = MARGIN_LEFT + PLOT_W + MARGIN_RIGHT
TOTAL_H = MARGIN_TOP + PLOT_H + MARGIN_BOTTOM

FONT = "'Inter', 'Helvetica Neue', Arial, sans-serif"
FONT_SIZE = 12
TITLE_SIZE = 14
TICK_SIZE = 11


def nice_ceil(v: float) -> float:
    """Round up to a 'nice' axis maximum."""
    if v <= 0:
        return 1.0
    mag = 10 ** math.floor(math.log10(v))
    for step in [1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10]:
        candidate = step * mag
        if candidate >= v:
            return candidate
    return v


def y_ticks(y_max: float) -> list[float]:
    """Generate ~5-7 evenly spaced tick values from 0 to y_max."""
    raw_step = y_max / 5
    mag = 10 ** math.floor(math.log10(raw_step)) if raw_step > 0 else 1
    for step in [1, 2, 2.5, 5, 10]:
        s = step * mag
        if y_max / s <= 8:
            ticks = []
            v = 0.0
            while v <= y_max + 1e-9:
                ticks.append(round(v, 2))
                v += s
            return ticks
    return [0, y_max]


def generate_svg(chart: dict) -> str:
    series = chart["series"]
    n_pts = len(DISTRIBUTIONS)

    # Compute y range
    all_vals = [v for _, data in series for v in data]
    y_max = nice_ceil(max(all_vals) * 1.08)

    # X positions (evenly spaced within plot area)
    x_step = PLOT_W / (n_pts - 1) if n_pts > 1 else PLOT_W
    xs = [MARGIN_LEFT + i * x_step for i in range(n_pts)]

    def y_pos(v: float) -> float:
        return MARGIN_TOP + PLOT_H - (v / y_max) * PLOT_H

    lines: list[str] = []

    # SVG header
    lines.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {TOTAL_W} {TOTAL_H}"'
        f' width="{TOTAL_W}" height="{TOTAL_H}"'
        f' style="background:#fff; font-family:{FONT}">'
    )

    # Title
    title_x = MARGIN_LEFT + PLOT_W / 2
    lines.append(
        f'  <text x="{title_x}" y="{MARGIN_TOP - 16}" text-anchor="middle"'
        f' font-size="{TITLE_SIZE}" font-weight="600" fill="#1e293b">'
        f'{chart["title"]}</text>'
    )

    # Gridlines + Y-axis ticks
    ticks = y_ticks(y_max)
    for t in ticks:
        yp = y_pos(t)
        lines.append(
            f'  <line x1="{MARGIN_LEFT}" y1="{yp:.1f}"'
            f' x2="{MARGIN_LEFT + PLOT_W}" y2="{yp:.1f}"'
            f' stroke="#e2e8f0" stroke-width="1"/>'
        )
        label = f"{t:g}"
        lines.append(
            f'  <text x="{MARGIN_LEFT - 8}" y="{yp + 4:.1f}" text-anchor="end"'
            f' font-size="{TICK_SIZE}" fill="#64748b">{label}</text>'
        )

    # X-axis labels
    for i, dist in enumerate(DISTRIBUTIONS):
        lines.append(
            f'  <text x="{xs[i]:.1f}" y="{MARGIN_TOP + PLOT_H + 20}"'
            f' text-anchor="middle" font-size="{TICK_SIZE}" fill="#64748b">'
            f'{dist}</text>'
        )

    # Y-axis label
    y_label_x = 14
    y_label_y = MARGIN_TOP + PLOT_H / 2
    lines.append(
        f'  <text x="{y_label_x}" y="{y_label_y}"'
        f' text-anchor="middle" font-size="{FONT_SIZE}" fill="#64748b"'
        f' transform="rotate(-90 {y_label_x} {y_label_y})">µs</text>'
    )

    # Plot area border (bottom + left axes)
    lines.append(
        f'  <line x1="{MARGIN_LEFT}" y1="{MARGIN_TOP + PLOT_H}"'
        f' x2="{MARGIN_LEFT + PLOT_W}" y2="{MARGIN_TOP + PLOT_H}"'
        f' stroke="#94a3b8" stroke-width="1.5"/>'
    )
    lines.append(
        f'  <line x1="{MARGIN_LEFT}" y1="{MARGIN_TOP}"'
        f' x2="{MARGIN_LEFT}" y2="{MARGIN_TOP + PLOT_H}"'
        f' stroke="#94a3b8" stroke-width="1.5"/>'
    )

    # Data lines + dots
    for name, data in series:
        colour = COLOURS[name]
        points = " ".join(f"{xs[i]:.1f},{y_pos(v):.1f}" for i, v in enumerate(data))
        lines.append(
            f'  <polyline points="{points}" fill="none"'
            f' stroke="{colour}" stroke-width="2.5" stroke-linejoin="round"/>'
        )
        for i, v in enumerate(data):
            lines.append(
                f'  <circle cx="{xs[i]:.1f}" cy="{y_pos(v):.1f}" r="3.5"'
                f' fill="{colour}" stroke="#fff" stroke-width="1.5"/>'
            )

    # Legend (right side)
    legend_x = MARGIN_LEFT + PLOT_W + 16
    legend_y_start = MARGIN_TOP + 10
    for idx, (name, _) in enumerate(series):
        colour = COLOURS[name]
        ly = legend_y_start + idx * 22
        # Colour line swatch
        lines.append(
            f'  <line x1="{legend_x}" y1="{ly}" x2="{legend_x + 20}" y2="{ly}"'
            f' stroke="{colour}" stroke-width="2.5"/>'
        )
        lines.append(
            f'  <circle cx="{legend_x + 10}" cy="{ly}" r="3"'
            f' fill="{colour}" stroke="#fff" stroke-width="1"/>'
        )
        # Label
        lines.append(
            f'  <text x="{legend_x + 26}" y="{ly + 4}" font-size="{TICK_SIZE}"'
            f' fill="#1e293b">{name}</text>'
        )

    lines.append("</svg>")
    return "\n".join(lines)


def main():
    out_dir = os.path.join(os.path.dirname(__file__), "charts")
    os.makedirs(out_dir, exist_ok=True)
    for chart in CHARTS:
        svg = generate_svg(chart)
        path = os.path.join(out_dir, chart["filename"])
        with open(path, "w") as f:
            f.write(svg)
        print(f"  wrote {path}")
    print(f"\nGenerated {len(CHARTS)} charts in {out_dir}/")


if __name__ == "__main__":
    main()
