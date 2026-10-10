#!/usr/bin/env python3
# Copyright 2025-2026 Angel Conde and the vecruntime contributors
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
"""Render the TPC-DS 3 TB page (docs/benchmarks/tpcds-3tb-graviton.html): VecRuntime against the Gluten + Velox
and DataFusion Comet results awslabs data-on-eks published for the same dataset and hardware.

    R=benchmarks/results/tpcds-sf3000-doeks-2026-10-10
    render-doeks-page.py --vector $R/vecruntime-p900.jsonl --vector-alt $R/vecruntime-p200.jsonl \\
        --gluten $R/doeks-gluten-velox-1.6.0-arm64.csv --comet $R/doeks-comet-0.16.0.csv \\
        --out docs/benchmarks/tpcds-3tb-graviton.html [--meta meta.json]

`--vector` / `--vector-alt` are the cluster runner's JSON-lines result files (the main run and the run at
data-on-eks's 200 shuffle partitions); `--gluten` / `--comet` are data-on-eks's per-query CSVs
(`Name,Mean,Min,Max`, seconds, names like `q1-v4.0`). `meta.json` overrides the text below (DEFAULT_META).
The helpers and the look are render-benchmark-page.py's; the page is self-contained (data inlined, Chart.js
from a CDN).
"""
import argparse, csv, html, importlib.util, json, math, sys
from pathlib import Path

sys.dont_write_bytecode = True  # importing the sibling script must not leave a __pycache__ in the repo
_spec = importlib.util.spec_from_file_location("render_benchmark_page", Path(__file__).with_name("render-benchmark-page.py"))
_base = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_base)
load, qkey, table = _base.load, _base.qkey, _base.table

DEFAULT_META = {
    "title": "VecRuntime vs Gluten + Velox vs DataFusion Comet on TPC-DS 3 TB, AWS Graviton4",
    "dataset": "TPC-DS scale factor 3000 (3 TB), Parquet on Amazon S3, generated with the data-on-eks generator image and arguments unchanged (103 query variants)",
    "cluster": "Amazon EKS; 12 x r8gd.12xlarge (48 vCPU AWS Graviton4, 384 GB, local NVMe as RAID0 for shuffle and spill), the data-on-eks benchmark's instance type and count",
    "executors": "23 executors x 5 cores x 58 GB each, as data-on-eks (Gluten / Comet: 20 GB heap + 6 GB overhead + 32 GB off-heap); VecRuntime: 44 GB heap + 14 GB overhead at 900 partitions, 35 GB + 23 GB at 200; driver 5 cores x 24 GB",
    "storage": "Amazon S3 through Hadoop 3.4.3 S3A with the Analytics Accelerator input stream",
    "versions": {
        "VecRuntime": "0.0.7 + #690 (merge join, #688) + #691 (join probe strings, #687); arm64 image",
        "Spark (VecRuntime)": "4.1.3, Scala 2.13, Amazon Corretto 25.0.4.1",
        "Gluten + Velox (data-on-eks)": "1.6.0 on Spark 3.5.8, Scala 2.12, JDK 17",
        "DataFusion Comet (data-on-eks)": "0.16.0 on Spark 3.5.8, Scala 2.12, JDK 17",
    },
    "vector_conf": [
        "spark.plugins=io.vecruntime.spark.VectorPlugin",
        "spark.shuffle.manager=org.apache.spark.sql.vecruntime.shuffle.VectorShuffleManager",
        "spark.sql.shuffle.partitions=900   (200 in the second run)",
        "spark.sql.adaptive.enabled=true",
        "spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold=64MB",
        "spark.vecruntime.exec.strictFloatingPoint=false",
        "spark.hadoop.fs.s3a.connection.maximum=1000, threads.max=256, max.total.tasks=128",
        "Executors: -XX:+UseParallelGC -XX:ParallelGCThreads=5 (data-on-eks runs ParallelGC too); ACCP as the JCA provider",
        "--add-modules=jdk.incubator.vector --enable-native-access=ALL-UNNAMED (driver and executors)",
    ],
    "notes": [
        "The Gluten and Comet numbers are data-on-eks's published results (the mean of their iterations); they were not re-run here. VecRuntime's are one measured iteration per query.",
        "The engines run on different Spark lines: VecRuntime needs Spark 4.1+ and JDK 25, the data-on-eks Gluten and Comet runs are on Spark 3.5.8 and JDK 17.",
        "data-on-eks leaves spark.sql.shuffle.partitions at Spark's default of 200 with AQE coalescing. VecRuntime is shown at 900 (its best setting in a tuning round) and at 200.",
        "At 200 partitions VecRuntime needs the larger overhead (35 GB + 23 GB): with 44 GB + 14 GB an executor exceeded its container in q93.",
        "There is no Apache Spark baseline at 3 TB, so result checksums are not compared against Spark here; VecRuntime's results match Spark's on the 1 TB runs and in the test suites.",
    ],
    "partition_diff_note": "19,301 rows at 900 partitions, 19,302 at 200. One group's coefficient of variation is exactly 1, so the order in which the final aggregate merges its partial moments (the order shuffle blocks arrive) decides whether it computes 1.0 or 1.0000000000000002, and only the second passes the query's cov > 1. Its row count can therefore differ by one between runs of the same configuration: repeat runs at 200 partitions returned 19,301, 19,301 and 19,302, and Spark's own CentralMomentAgg is order-dependent in the same way (Spark returned 19,301 in the one run made). See #695.",
    "results_doc": "https://github.com/vecruntime/vecruntime/blob/main/docs/results.md",
    "repo": "https://github.com/vecruntime/vecruntime",
    "doeks": "https://github.com/awslabs/data-on-eks",
    "run_doc": "https://github.com/vecruntime/vecruntime/blob/main/benchmarks/k8s/README.md",
    "graviton_page": "tpcds-1tb-graviton.html",
}

ENGINES = ("vector", "alt", "gluten", "comet")
COLORS = {"vector": "#2563eb", "alt": "#93c5fd", "gluten": "#16a34a", "comet": "#f59e0b"}
LABELS = {"vector": "VecRuntime (900 partitions)", "alt": "VecRuntime (200 partitions)",
          "gluten": "Gluten + Velox 1.6.0", "comet": "DataFusion Comet 0.16.0"}


def load_csv(path):
    """Seconds per query from a data-on-eks result CSV (Name,Mean,...; names like q1-v4.0)."""
    return {r["Name"].split("-")[0]: float(r["Mean"]) for r in csv.DictReader(open(path))}


def geomean(xs):
    xs = [x for x in xs if x > 0]
    return math.exp(sum(math.log(x) for x in xs) / len(xs))


def bucket(ratio):
    """A query's time ratio (VecRuntime / other) as a distribution bucket."""
    if ratio <= 0.5: return "2x+ faster"
    if ratio <= 0.8: return "1.25-2x faster"
    if ratio < 1.0: return "up to 1.25x faster"
    if ratio < 1.25: return "up to 1.25x slower"
    return "1.25x+ slower"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vector", required=True); ap.add_argument("--vector-alt", required=True)
    ap.add_argument("--gluten", required=True); ap.add_argument("--comet", required=True)
    ap.add_argument("--out", required=True); ap.add_argument("--meta")
    ap.add_argument("--web-out", help="also write a standalone web/ page (no Jekyll) to this path")
    ap.add_argument("--web-base", default="/vecruntime", help="base URL prefix for the web/ page (default /vecruntime)")
    a = ap.parse_args()
    meta = dict(DEFAULT_META)
    if a.meta:
        meta.update(json.loads(Path(a.meta).read_text()))
    V, A = load(a.vector), load(a.vector_alt)
    sec = {"vector": {q: r["medianMs"] / 1000 for q, r in V.items()}, "alt": {q: r["medianMs"] / 1000 for q, r in A.items()},
           "gluten": load_csv(a.gluten), "comet": load_csv(a.comet)}
    queries = sorted(set.intersection(*(set(sec[e]) for e in ENGINES)), key=qkey)
    if len(queries) < 100:
        sys.exit(f"only {len(queries)} queries common to the four result sets")
    n = len(queries)
    s = {e: {q: sec[e][q] for q in queries} for e in ENGINES}
    tot = {e: sum(s[e].values()) for e in ENGINES}
    vs = lambda e, other, q: s[other][q] / s[e][q]  # how many times faster e is than other on q
    geo = {(e, o): geomean([vs(e, o, q) for q in queries]) for e in ("vector", "alt") for o in ("gluten", "comet")}
    wins = {(e, o): sum(1 for q in queries if s[e][q] < s[o][q]) for e in ("vector", "alt") for o in ("gluten", "comet")}
    rows_diff = [q for q in queries if V[q].get("rows") != A[q].get("rows")]
    cks_diff = [q for q in queries if V[q].get("checksum") != A[q].get("checksum")]
    exe = {e: sum(R[q].get("executorRunTimeMs", 0) for q in queries) / 3.6e6 for e, R in (("vector", V), ("alt", A))}
    gc = {e: sum(R[q].get("gcTimeMs", 0) for q in queries) / 3.6e6 for e, R in (("vector", V), ("alt", A))}
    shuf = {e: sum(R[q].get("shuffleReadBytes", 0) for q in queries) / 1e12 for e, R in (("vector", V), ("alt", A))}

    f1 = lambda x: f"{x:,.1f}"

    def summary_row(e):
        if e in ("vector", "alt"):
            return (LABELS[e], f"<b>{f1(tot[e])}</b>" if e == "vector" else f1(tot[e]),
                    f"{tot['gluten'] / tot[e]:.2f}x", f"{tot['comet'] / tot[e]:.2f}x",
                    f"{geo[(e, 'gluten')]:.2f}x / {geo[(e, 'comet')]:.2f}x",
                    f"{wins[(e, 'gluten')]} / {wins[(e, 'comet')]}")
        return (LABELS[e], f1(tot[e]), "", "", "", "")

    def dist(o):
        order = ["2x+ faster", "1.25-2x faster", "up to 1.25x faster", "up to 1.25x slower", "1.25x+ slower"]
        c = {k: 0 for k in order}
        for q in queries:
            c[bucket(s["vector"][q] / s[o][q])] += 1
        return c

    dg, dc = dist("gluten"), dist("comet")
    behind = sorted([q for q in queries if s["vector"][q] > min(s["gluten"][q], s["comet"][q])],
                    key=lambda q: -(s["vector"][q] - min(s["gluten"][q], s["comet"][q])))

    def behind_rows():
        return [(q, f1(s["vector"][q]), f1(s["alt"][q]), f1(s["gluten"][q]), f1(s["comet"][q]),
                 f"{s['vector'][q] - min(s['gluten'][q], s['comet'][q]):+.1f}") for q in behind]

    top = sorted(queries, key=lambda q: -vs("vector", "gluten", q))[:10]
    top_rows = [(q, f1(s["vector"][q]), f1(s["gluten"][q]), f1(s["comet"][q]),
                 f"<b>{vs('vector', 'gluten', q):.2f}x</b>", f"{vs('vector', 'comet', q):.2f}x") for q in top]
    by_parts = sorted(queries, key=lambda q: s["alt"][q] - s["vector"][q])
    parts_rows = lambda qs: [(q, f1(s["vector"][q]), f1(s["alt"][q]), f"{s['alt'][q] - s['vector'][q]:+.1f}") for q in qs]

    per_query_rows = []
    for q in queries:
        best = min(s[e][q] for e in ENGINES)
        cell = lambda x: f"<b>{f1(x)}</b>" if x == best else f1(x)
        per_query_rows.append((q, cell(s["vector"][q]), cell(s["alt"][q]), cell(s["gluten"][q]), cell(s["comet"][q]),
                               f"{vs('vector', 'gluten', q):.2f}x", f"{vs('vector', 'comet', q):.2f}x"))

    chart_data = {"queries": queries, "series": {e: [round(s[e][q], 2) for q in queries] for e in ENGINES},
                  "labels": LABELS, "colors": COLORS, "totals": {e: round(tot[e], 1) for e in ENGINES}}
    versions = table(["Component", "Version"], [(k, html.escape(v)) for k, v in meta["versions"].items()], "kv")
    env = table(["Component", "Configuration"], [(k, html.escape(meta[k.lower()])) for k in ("Dataset", "Cluster", "Executors", "Storage")], "kv")
    conf_v = "\n".join(meta["vector_conf"])
    notes = "".join(f"<li>{html.escape(x)}</li>" for x in meta["notes"])

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
<a href="{meta["doeks"]}">awslabs data-on-eks</a> publishes TPC-DS 3 TB results for two native engines, Gluten + Velox and DataFusion Comet,
on twelve Graviton4 nodes. This page runs VecRuntime on the same generated data, the same instance type and count and the same executor layout,
and sets its times against theirs.</p>

<div class="cards">
<div class="card"><div class="l">VecRuntime, 900 partitions</div><div class="n">{tot["vector"]:,.0f} s</div><div class="l">{n} queries · {tot["gluten"] / tot["vector"]:.2f}x Gluten · {tot["comet"] / tot["vector"]:.2f}x Comet</div></div>
<div class="card"><div class="l">VecRuntime, 200 partitions</div><div class="n">{tot["alt"]:,.0f} s</div><div class="l">data-on-eks's setting · {tot["gluten"] / tot["alt"]:.2f}x Gluten · {tot["comet"] / tot["alt"]:.2f}x Comet</div></div>
<div class="card"><div class="l">Gluten + Velox 1.6.0 / Comet 0.16.0</div><div class="n">{tot["gluten"]:,.0f} / {tot["comet"]:,.0f} s</div><div class="l">data-on-eks's published runs</div></div>
</div>

<h2 id="summary">Summary</h2>
{table(["Engine", "Completion time (s)", "vs Gluten", "vs Comet", "Per-query geomean vs Gluten / Comet", "Faster than Gluten / Comet on"],
       [summary_row(e) for e in ENGINES])}
<div class="chart"><canvas id="totals"></canvas></div>

<h3>Where each engine is ahead</h3>
<div class="two">
<div>{table(["VecRuntime (900) vs Gluten", "Queries"], list(dg.items()))}</div>
<div>{table(["VecRuntime (900) vs Comet", "Queries"], list(dc.items()))}</div>
</div>
<div class="chart tall"><canvas id="speed"></canvas></div>

<h3>Largest leads</h3>
{table(["Query", "VecRuntime (s)", "Gluten (s)", "Comet (s)", "vs Gluten", "vs Comet"], top_rows)}
<h3>Where VecRuntime is behind</h3>
<p>Queries where Gluten or Comet is faster than VecRuntime at 900 partitions, by the gap to the faster of the two (seconds).</p>
{table(["Query", "VecRuntime 900", "VecRuntime 200", "Gluten", "Comet", "Gap"], behind_rows()) if behind else "<p>None.</p>"}

<h2 id="partitions">900 against 200 shuffle partitions</h2>
<p>data-on-eks runs at Spark's default of 200 shuffle partitions. At 3 TB VecRuntime is {100 * tot["alt"] / tot["vector"] - 100:.1f}% slower in total at 200
than at 900 ({f1(tot["alt"])} s against {f1(tot["vector"])} s); the per-query differences go both ways.
{"Both runs returned the same rows and checksums on every query." if not rows_diff and not cks_diff else
 "The two runs returned the same rows and checksums on every query except " + ", ".join(sorted(set(rows_diff) | set(cks_diff), key=qkey)) + ": " + html.escape(meta.get("partition_diff_note") or "see the notes below.")}</p>
<div class="two">
<div><p>Slower at 200:</p>{table(["Query", "900 (s)", "200 (s)", "Difference"], parts_rows(list(reversed(by_parts[-8:]))))}</div>
<div><p>Faster at 200:</p>{table(["Query", "900 (s)", "200 (s)", "Difference"], parts_rows(by_parts[:8]))}</div>
</div>
{table(["VecRuntime run", "Executor time (h)", "GC (h)", "Shuffle read (TB)"], [
    (LABELS["vector"], f"{exe['vector']:.1f}", f"{gc['vector']:.2f}", f"{shuf['vector']:.2f}"),
    (LABELS["alt"], f"{exe['alt']:.1f}", f"{gc['alt']:.2f}", f"{shuf['alt']:.2f}"),
])}

<h2 id="infrastructure">Benchmark infrastructure</h2>
<p><b>Methodology.</b> The dataset was generated with data-on-eks's own generator image and arguments, and VecRuntime ran on the node type,
node count and executor layout of their benchmark (23 executors x 5 cores, 58 GB each). The two VecRuntime runs ran one after another on the
same nodes, each alone on the cluster; each query ran once after the plan was compiled, and its time is the wall-clock of its execution as the
runner measures it. Before the full runs, a tuning round on ten heavy queries chose the heap/overhead split; it found two problems that the
fixes in #690 and #691 address (q14a: 224 s to 56 s; q24a: 141 s to 105 s).</p>
<h3>Test environment</h3>
{env}
<h3>Versions</h3>
{versions}
<h3>VecRuntime configuration</h3>
<pre>{html.escape(conf_v)}</pre>
<h3>Notes</h3>
<ul>{notes}</ul>

<h2 id="results">Per query</h2>
<p>Seconds per query (hover for values; click a legend entry to hide an engine).</p>
<div class="chart tall"><canvas id="perquery1"></canvas></div>
<div class="chart tall"><canvas id="perquery2"></canvas></div>
<div class="chart tall"><canvas id="perquery3"></canvas></div>

<h2 id="table">All queries</h2>
<details open><summary>Seconds per query, the fastest engine in bold</summary>
{table(["Query", "VecRuntime 900", "VecRuntime 200", "Gluten", "Comet", "vs Gluten", "vs Comet"], per_query_rows, "all")}
</details>

<h2 id="running">Running the benchmark</h2>
<p>The cluster runner, the Spark-on-Kubernetes manifests and the image are in <a href="{meta["run_doc"]}">the benchmark runner's README</a>;
the image builds for arm64 on an arm64 node. <code>run-matrix.sh</code> writes one JSON-lines result file per run; this page is rendered from two
of them and data-on-eks's two result CSVs by <code>benchmarks/scripts/render-doeks-page.py</code>. The 1 TB runs against Apache Spark are on the
<a href="{meta["graviton_page"]}">Graviton4 1 TB page</a>.</p>

<p class="foot">TPC-DS is a benchmark of the Transaction Processing Performance Council; these results are not audited TPC results and are not comparable to
published TPC-DS results. VecRuntime's times are one measured iteration per query; run-to-run variation on the heavy queries is a few percent.</p>
</div>
<script>
const D = {json.dumps(chart_data)};
const opts = (title, yTitle, extra={{}}) => Object.assign({{
  responsive: true, maintainAspectRatio: false, interaction: {{ mode: 'index', intersect: false }},
  plugins: {{ title: {{ display: !!title, text: title }}, legend: {{ position: 'top' }} }},
  scales: {{ y: {{ title: {{ display: true, text: yTitle }} }} }}
}}, extra);
const all = ['vector', 'alt', 'gluten', 'comet'];
new Chart(document.getElementById('totals'), {{ type: 'bar', data: {{ labels: all.map(e => D.labels[e]),
  datasets: [{{ label: 'Completion time', data: all.map(e => D.totals[e]), backgroundColor: all.map(e => D.colors[e]) }}] }},
  options: opts('Completion time, {n} queries', 'seconds', {{ plugins: {{ legend: {{ display: false }}, title: {{ display: true, text: 'Completion time, {n} queries' }} }} }}) }});
const shown = ['vector', 'gluten', 'comet'];
const chunks = [D.queries.slice(0, 35), D.queries.slice(35, 70), D.queries.slice(70)];
chunks.forEach((keys, i) => new Chart(document.getElementById('perquery' + (i + 1)), {{ type: 'bar',
  data: {{ labels: keys, datasets: shown.map(e => ({{ label: D.labels[e], backgroundColor: D.colors[e], borderColor: D.colors[e],
    data: keys.map(q => D.series[e][D.queries.indexOf(q)]) }})) }},
  options: opts(i === 0 ? 'Seconds per query' : '', 'seconds') }}));
new Chart(document.getElementById('speed'), {{ type: 'line',
  data: {{ labels: D.queries, datasets: ['gluten', 'comet'].map(o => ({{ label: 'vs ' + D.labels[o], borderColor: D.colors[o], backgroundColor: D.colors[o],
    pointRadius: 3, showLine: false, data: D.queries.map((q, i) => +(D.series[o][i] / D.series.vector[i]).toFixed(3)) }})) }},
  options: opts('VecRuntime (900 partitions) speedup per query (log scale; 1 = parity)', 'x faster',
    {{ scales: {{ y: {{ type: 'logarithmic', min: 0.25, max: 20, title: {{ display: true, text: 'x faster' }} }} }} }}) }});
</script>
"""
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(page)
    print(f"wrote {a.out}: {n} queries; vector={tot['vector']:.1f}s alt={tot['alt']:.1f}s gluten={tot['gluten']:.1f}s comet={tot['comet']:.1f}s; "
          f"geomean vs gluten {geo[('vector', 'gluten')]:.3f} vs comet {geo[('vector', 'comet')]:.3f}; rows differ {rows_diff}; checksums differ {cks_diff}")
    if a.web_out:
        from webwrap import to_web_page
        Path(a.web_out).parent.mkdir(parents=True, exist_ok=True)
        Path(a.web_out).write_text(to_web_page(page, a.web_base, description=meta.get("dataset", "")))
        print(f"wrote {a.web_out}: web/ page under {a.web_base}")


if __name__ == "__main__":
    main()
