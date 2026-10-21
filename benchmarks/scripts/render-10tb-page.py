#!/usr/bin/env python3
# Copyright 2026-2027 Angel Conde and the vecruntime contributors
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing permissions and limitations under the
# License.
"""Render the TPC-DS 10 TB page (docs/benchmarks/tpcds-10tb-graviton.html): VecRuntime's 99-query total and per-query
times on 11 x m8gd.4xlarge, set against the totals other engines publish for the same instance type and count.

    R=benchmarks/results/tpcds-sf10000-2026-10-21
    render-10tb-page.py --vector $R/vecruntime-pass2.jsonl --vector-alt $R/vecruntime-pass1.jsonl \\
        --published $R/published.json --meta $R/meta.json --out docs/benchmarks/tpcds-10tb-graviton.html

`--vector` / `--vector-alt` are the cluster runner's JSON-lines result files (103 query variants; the page reports the
99 TPC-DS queries, q14, q23, q24 and q39 as the sums of their a and b parts). `--published` lists other engines' totals
(name, seconds) with their source; only totals are compared. `meta.json` holds the page's text. The helpers and the look
are render-benchmark-page.py's; the page is self-contained (data inlined, Chart.js from a CDN).
"""
import argparse, html, importlib.util, json, re, sys
from pathlib import Path

sys.dont_write_bytecode = True  # importing the sibling script must not leave a __pycache__ in the repo
_spec = importlib.util.spec_from_file_location("render_benchmark_page", Path(__file__).with_name("render-benchmark-page.py"))
_base = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_base)
load, table = _base.load, _base.table

VECTOR_COLOR, ALT_COLOR, OTHER_COLOR = "#2563eb", "#93c5fd", "#9ca3af"


def official(rows):
    """The 99 TPC-DS queries from the runner's 103 variants: q14, q23, q24 and q39 as a + b. Seconds per query."""
    out = {}
    for q, r in rows.items():
        name = re.sub(r"[ab]$", "", q)
        out[name] = out.get(name, 0.0) + r["medianMs"] / 1000
    return out


def sums(rows, field):
    return sum(r.get(field, 0) for r in rows.values())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vector", required=True); ap.add_argument("--vector-alt")
    ap.add_argument("--published", required=True); ap.add_argument("--meta", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--web-out", help="also write a standalone web/ page (no Jekyll) to this path")
    ap.add_argument("--web-base", default="/vecruntime", help="base URL prefix for the web/ page (default /vecruntime)")
    a = ap.parse_args()
    meta = json.loads(Path(a.meta).read_text())
    pub = json.loads(Path(a.published).read_text())
    V = load(a.vector)
    A = load(a.vector_alt) if a.vector_alt else None
    s = official(V)
    s2 = official(A) if A else None
    queries = sorted(s, key=lambda q: int(q[1:]))
    if len(queries) != 99:
        sys.exit(f"expected the 99 TPC-DS queries, got {len(queries)}")
    if s2 is not None and set(s2) != set(s):
        sys.exit("the two VecRuntime runs cover different queries")
    tot = sum(s.values())
    tot2 = sum(s2.values()) if s2 else None
    f1 = lambda x: f"{x:,.1f}"
    f0 = lambda x: f"{x:,.0f}"

    # Totals: the published engines and VecRuntime, slowest first (the order the reference page uses).
    bars = [(e["name"], float(e["seconds"]), OTHER_COLOR) for e in pub["engines"]]
    bars.append(("VecRuntime", tot, VECTOR_COLOR))
    bars.sort(key=lambda b: -b[1])
    spark = next((e["seconds"] for e in pub["engines"] if "Spark" in e["name"]), None)
    total_rows = [(("<b>VecRuntime</b>" if n == "VecRuntime" else html.escape(n)),
                   f"<b>{f0(t)}</b>" if n == "VecRuntime" else f0(t),
                   f"{t / tot:.2f}x" if n != "VecRuntime" else "",
                   "this page" if n == "VecRuntime" else "published")
                  for n, t, _ in bars]

    exe = sums(V, "executorRunTimeMs") / 3.6e6
    gc = sums(V, "gcTimeMs") / 3.6e6
    spill = sums(V, "spillBytes") / 2 ** 30
    shuf = sums(V, "shuffleWriteBytes") / 2 ** 40
    top = sorted(queries, key=lambda q: -s[q])[:10]
    top_rows = [(q, f1(s[q]), f"{100 * s[q] / tot:.1f}%") for q in top]
    per_rows = [(q, f1(s[q])) + ((f1(s2[q]), f"{s2[q] - s[q]:+.1f}") if s2 else ()) for q in queries]

    chart = {"bars": [{"name": n, "seconds": round(t, 1), "color": c} for n, t, c in bars],
             "queries": queries, "pass2": [round(s[q], 2) for q in queries],
             "pass1": [round(s2[q], 2) for q in queries] if s2 else None}
    versions = table(["Component", "Version"], [(k, html.escape(v)) for k, v in meta["versions"].items()], "kv")
    env = table(["Component", "Configuration"],
                [(k, html.escape(meta[k.lower()])) for k in ("Dataset", "Cluster", "Executors", "Storage")], "kv")
    notes = "".join(f"<li>{html.escape(x)}</li>" for x in meta["notes"])
    spark_card = (f'<div class="card"><div class="l">vs OSS Apache Spark (published)</div><div class="n">{spark / tot:.1f}x</div>'
                  f'<div class="l">{f0(spark)} s against {f0(tot)} s</div></div>' if spark else "")
    pass_card = (f'<div class="card"><div class="l">Second pass / first pass</div><div class="n">{f0(tot)} / {f0(tot2)} s</div>'
                 f'<div class="l">{100 * abs(tot2 - tot) / tot:.1f}% apart</div></div>' if s2 else "")

    page = f"""---
layout: default
title: {json.dumps(meta["title"])}
---
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
/* Benchmark-page components. Scoped under .bench so they never restyle the site
   shell; colours come from the site theme variables (light/dark toggle works). */
.doc:has(.bench) {{ max-width: none; }}
.bench .lede {{ color: var(--muted); }}
.bench .cards {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 14px; margin: 1em 0; }}
.bench .card {{ background: var(--card); border: 1px solid var(--line); border-radius: 8px; padding: 14px 16px; }}
.bench .card .n {{ font-size: 1.7rem; font-weight: 700; }} .bench .card .l {{ color: var(--muted); font-size: .9rem; }}
.bench table td:nth-child(n+2) {{ font-variant-numeric: tabular-nums; }}
.bench table.kv td:first-child {{ font-weight: 600; white-space: nowrap; }}
.bench .chart {{ position: relative; height: 340px; margin: 1em 0 2em; }} .bench .chart.tall {{ height: 420px; }}
.bench .two {{ display:grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 24px; }} .bench .two > * {{ min-width: 0; }} @media (max-width: 860px) {{ .bench .two {{ grid-template-columns: minmax(0, 1fr); }} }}
.bench details summary {{ cursor: pointer; font-weight: 600; margin: 1em 0; }}
.bench .foot {{ color: var(--muted); font-size: .85rem; margin-top: 3em; }}
</style>
<div class="bench">
<h1>{html.escape(meta["title"])}</h1>
<p class="lede"><a href="{meta["repo"]}">VecRuntime</a> runs Spark SQL's filters, projections, aggregates, sorts and joins on Arrow-layout
batches with the Java Vector API -- on the JVM, no native code -- and moves batches between executors over its own Arrow Flight shuffle.
This page is its TPC-DS 10 TB run on eleven Graviton4 nodes, set against the totals <a href="{pub["source"]}">{html.escape(pub["source_label"])}</a>
publishes for other engines on the same instance type and count.</p>

<div class="cards">
<div class="card"><div class="l">VecRuntime, 99 queries</div><div class="n">{f0(tot)} s</div><div class="l">10 TB · {html.escape(pub["hardware"])}</div></div>
{spark_card}
{pass_card}
</div>

<h2 id="summary">Total runtime</h2>
<p>Lower is better. The grey bars are published totals, not re-run here (see the notes); VecRuntime's is this page's run.</p>
<div class="chart"><canvas id="totals"></canvas></div>
{table(["Engine", "Total (s)", "Its time / VecRuntime's", "Source"], total_rows)}
<p class="lede">{html.escape(pub["note"])}</p>

<h2 id="where">Where the time goes</h2>
<div class="two">
<div><p>The ten longest queries:</p>{table(["Query", "Seconds", "Share of total"], top_rows)}</div>
<div><p>Across the 99 queries:</p>{table(["Measure", "Value"], [
    ("Executor time", f"{exe:,.1f} h"), ("GC time", f"{gc:.2f} h"),
    ("Spill", f"{spill:,.0f} GiB"), ("Shuffle written", f"{shuf:.2f} TiB")], "kv")}</div>
</div>

<h2 id="results">Per query</h2>
<p>Seconds per query{", both passes" if s2 else ""} (hover for values).</p>
<div class="chart tall"><canvas id="perquery1"></canvas></div>
<div class="chart tall"><canvas id="perquery2"></canvas></div>
<div class="chart tall"><canvas id="perquery3"></canvas></div>

<h2 id="infrastructure">Benchmark infrastructure</h2>
<p><b>Methodology.</b> {html.escape(meta["methodology"])}</p>
<h3>Test environment</h3>
{env}
<h3>Versions</h3>
{versions}
<h3>VecRuntime configuration</h3>
<pre>{html.escape(chr(10).join(meta["vector_conf"]))}</pre>
<h3>Notes</h3>
<ul>{notes}</ul>

<h2 id="table">All queries</h2>
<details><summary>Seconds per query</summary>
{table(["Query", "Pass 2 (s)"] + (["Pass 1 (s)", "Difference"] if s2 else []), per_rows, "all")}
</details>

<h2 id="running">Running the benchmark</h2>
<p>The cluster runner, the Spark-on-Kubernetes manifests and the image are in <a href="{meta["run_doc"]}">the benchmark runner's README</a>.
<code>run-matrix.sh</code> writes one JSON-lines result file per run; this page is rendered from them by
<code>benchmarks/scripts/render-10tb-page.py</code>. The runs and their memory measurements are discussed in <a href="{meta["issue"]}">#869</a>.</p>

<p class="foot">TPC-DS is a benchmark of the Transaction Processing Performance Council; these results are not audited TPC results and are not comparable to
published TPC-DS results. VecRuntime's times are one measured iteration per query. The other engines' totals are their publishers' figures.</p>
</div>
<script>
const D = {json.dumps(chart)};
const opts = (title, yTitle, extra={{}}) => Object.assign({{
  responsive: true, maintainAspectRatio: false, interaction: {{ mode: 'index', intersect: false }},
  plugins: {{ title: {{ display: !!title, text: title }}, legend: {{ position: 'top' }} }},
  scales: {{ y: {{ title: {{ display: true, text: yTitle }} }} }}
}}, extra);
new Chart(document.getElementById('totals'), {{ type: 'bar',
  data: {{ labels: D.bars.map(b => b.name), datasets: [{{ label: 'Total runtime', data: D.bars.map(b => b.seconds),
    backgroundColor: D.bars.map(b => b.color) }}] }},
  options: {{ indexAxis: 'y', responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ display: false }}, title: {{ display: true, text: 'Total runtime, 99 queries, 10 TB (seconds, lower is better)' }} }},
    scales: {{ x: {{ title: {{ display: true, text: 'seconds' }} }} }} }} }});
const runs = [{{ label: 'VecRuntime, pass 2', color: '{VECTOR_COLOR}', data: D.pass2 }}]
  .concat(D.pass1 ? [{{ label: 'VecRuntime, pass 1', color: '{ALT_COLOR}', data: D.pass1 }}] : []);
const chunks = [[0, 33], [33, 66], [66, 99]];
chunks.forEach(([lo, hi], i) => new Chart(document.getElementById('perquery' + (i + 1)), {{ type: 'bar',
  data: {{ labels: D.queries.slice(lo, hi), datasets: runs.map(r => ({{ label: r.label, backgroundColor: r.color, borderColor: r.color,
    data: r.data.slice(lo, hi) }})) }},
  options: opts(i === 0 ? 'Seconds per query' : '', 'seconds') }}));
</script>
"""
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(page)
    print(f"wrote {a.out}: 99 queries; pass 2 {tot:.1f}s" + (f", pass 1 {tot2:.1f}s" if s2 else ""))
    if a.web_out:
        from webwrap import to_web_page
        Path(a.web_out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.web_out).write_text(to_web_page(page, a.web_base, description=meta.get("dataset", "")))
        print(f"wrote {a.web_out}: web/ page under {a.web_base}")


if __name__ == "__main__":
    main()
