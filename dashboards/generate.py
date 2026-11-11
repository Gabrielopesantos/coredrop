"""Generate dashboards/coredrop.json, the Grafana dashboard for coredrop.

Edit this file, not the JSON, then run from the repo root:

    python3 dashboards/generate.py > dashboards/coredrop.json

CI fails when the JSON is not the output of this script.
"""

import json
import sys

DS = {"type": "prometheus", "uid": "${datasource}"}
S = 'cluster=~"$cluster",namespace=~"$namespace",pod=~"$pod"'
pid = 0


def nid():
    global pid
    pid += 1
    return pid


def target(expr, legend="", instant=False, fmt="time_series", ref="A"):
    t = {"datasource": DS, "expr": expr, "refId": ref, "legendFormat": legend or "__auto"}
    if instant:
        t.update({"instant": True, "range": False, "format": fmt})
    else:
        t.update({"range": True})
    return t


def row(title, y):
    return {"type": "row", "id": nid(), "title": title, "collapsed": False,
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []}


def stat(title, desc, expr, x, y, w=4, unit="short", red_above=None, decimals=0):
    steps = [{"color": "green", "value": None}]
    if red_above is not None:
        steps.append({"color": "red", "value": red_above})
    return {
        "type": "stat", "id": nid(), "title": title, "description": desc, "datasource": DS,
        "gridPos": {"h": 4, "w": w, "x": x, "y": y},
        "targets": [target(expr, instant=True)],
        "fieldConfig": {"defaults": {"unit": unit, "decimals": decimals,
                                     "thresholds": {"mode": "absolute", "steps": steps},
                                     "color": {"mode": "thresholds"}},
                        "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "none", "textMode": "auto",
                    "justifyMode": "auto", "orientation": "auto"},
    }


def ts(title, desc, targets, x, y, w, h=8, unit="short", bars=False, stack=False, interval=None):
    custom = {"drawStyle": "bars" if bars else "line", "fillOpacity": 80 if bars else 10,
              "lineWidth": 1, "showPoints": "never", "spanNulls": False,
              "stacking": {"mode": "normal" if stack else "none", "group": "A"}}
    p = {
        "type": "timeseries", "id": nid(), "title": title, "description": desc, "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": targets,
        "fieldConfig": {"defaults": {"unit": unit, "min": 0, "custom": custom,
                                     "color": {"mode": "palette-classic"}},
                        "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }
    if interval:
        p["interval"] = interval
    return p


def result_colors():
    return [
        {"matcher": {"id": "byName", "options": "ok"},
         "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "green"}}]},
        {"matcher": {"id": "byName", "options": "error"},
         "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "red"}}]},
    ]


def table(title, desc, expr, x, y, w, h, exclude, rename=None):
    return {
        "type": "table", "id": nid(), "title": title, "description": desc, "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": [target(expr, instant=True, fmt="table")],
        "fieldConfig": {"defaults": {"custom": {"align": "auto"}}, "overrides": []},
        "options": {"showHeader": True, "cellHeight": "sm"},
        "transformations": [{"id": "organize", "options": {
            "excludeByName": {k: True for k in exclude},
            "renameByName": rename or {}}}],
    }


panels = []
y = 0

# --- Overview ---------------------------------------------------------------
panels.append(row("Overview", y)); y += 1
panels += [
    stat("Daemons reporting",
         "Number of coredrop daemon pods that Prometheus scrapes now. Expect one for each node.",
         f"count(coredrop_build_info{{{S}}})", 0, y, w=3),
    stat("core_pattern lost",
         "Daemons whose node core_pattern did not route cores to coredrop at some time in the time range. Another tool wrote core_pattern, and crashes were not captured until the daemon installed it again. Expect 0.",
         f"count(min_over_time(coredrop_core_pattern_installed{{{S}}}[$__range]) == 0) or vector(0)", 3, y, w=3, red_above=1),
    stat("Captures uploaded",
         "Captures with outcome uploaded in the time range. Prometheus counts only the captures after its first scrape of a daemon.",
         f'sum(increase(coredrop_captures_total{{{S},outcome="uploaded"}}[$__range]))', 6, y, w=3),
    stat("Captures failed",
         "Captures with outcome failed in the time range. The capture log on the daemon pod has the error.",
         f'sum(increase(coredrop_captures_total{{{S},outcome="failed"}}[$__range]))', 9, y, w=3, red_above=1),
    stat("Spool full",
         "Captures that kept no core or a truncated core because the spool was full, in the time range.",
         f'sum(increase(coredrop_captures_total{{{S},outcome="spool-full"}}[$__range]))', 12, y, w=3, red_above=1),
    stat("Upload success",
         "Upload attempts with result ok, divided by all upload attempts, in the time range. Empty when there were no attempts.",
         f'sum(increase(coredrop_upload_attempts_total{{{S},result="ok"}}[$__range]))'
         f" / sum(increase(coredrop_upload_attempts_total{{{S}}}[$__range]))",
         15, y, w=3, unit="percentunit", decimals=1),
    stat("Oldest spooled capture",
         "Age of the oldest capture that waits for upload, on all selected nodes. 0 when the spools are empty.",
         f"max(coredrop_spool_oldest_bundle_age_seconds{{{S}}})", 18, y, w=3, unit="s", red_above=900),
    stat("Spool size",
         "Size of the spool on all selected nodes now: incoming/ and ready/.",
         f"sum(coredrop_spool_bytes{{{S}}})", 21, y, w=3, unit="bytes", decimals=None),
]
# Upload success is good when high: red below 90%.
panels[-3]["fieldConfig"]["defaults"]["thresholds"]["steps"] = [
    {"color": "red", "value": None}, {"color": "orange", "value": 0.9}, {"color": "green", "value": 0.99}]
y += 4

# --- Captures ---------------------------------------------------------------
panels.append(row("Captures", y)); y += 1
panels.append(ts(
    "Captures by outcome",
    "Captures in each interval by final outcome. A spooled capture is counted once, when it is done. increase() extrapolates to the edges of each interval, so a bar can show a fraction.",
    [target(f"sum by (outcome) (increase(coredrop_captures_total{{{S}}}[$__interval]))", "{{outcome}}")],
    0, y, 8, bars=True, stack=True, interval="1m"))
panels.append(ts(
    "Core bytes",
    "Core bytes of done captures per second: read is the uncompressed size from the kernel, stored is the compressed size in the object store.",
    [target(f"sum(rate(coredrop_core_bytes_total{{{S}}}[$__rate_interval]))", "read", ref="A"),
     target(f"sum(rate(coredrop_stored_bytes_total{{{S}}}[$__rate_interval]))", "stored", ref="B")],
    8, y, 8, unit="Bps"))
panels.append(ts(
    "Truncated cores",
    "Done captures with a truncated core in each interval: the core was larger than the size cap, the spool was full, or the stream failed. The manifest has the reason.",
    [target(f"sum by (cluster, node) (increase(coredrop_cores_truncated_total{{{S}}}[$__interval]))", "{{cluster}} {{node}}")],
    16, y, 8, bars=True, interval="1m"))
y += 8
panels.append(table(
    "Captures by node",
    "Captures in the time range for each node and outcome. Rows with 0 are not shown.",
    f"sum by (cluster, namespace, node, outcome) (increase(coredrop_captures_total{{{S}}}[$__range])) > 0",
    0, y, 24, 6, ["Time"], {"Value": "captures"}))
y += 6

# --- Upload and spool -------------------------------------------------------
panels.append(row("Upload and spool", y)); y += 1
up = ts("Upload attempts",
        "Upload attempts of spooled captures per minute, by result. Retries count as attempts.",
        [target(f"sum by (result) (rate(coredrop_upload_attempts_total{{{S}}}[$__rate_interval])) * 60", "{{result}}")],
        0, y, 6, unit="short")
up["fieldConfig"]["overrides"] = result_colors()
panels.append(up)
panels.append(ts(
    "Upload duration",
    "Time to upload one spooled capture (core, proc snapshot and manifest), over the last 10 minutes. Empty when there was no upload.",
    [target(f"histogram_quantile(0.5, sum by (le) (rate(coredrop_upload_duration_seconds_bucket{{{S}}}[10m])))", "p50", ref="A"),
     target(f"histogram_quantile(0.95, sum by (le) (rate(coredrop_upload_duration_seconds_bucket{{{S}}}[10m])))", "p95", ref="B")],
    6, y, 6, unit="s"))
size = ts(
    "Spool size",
    "Size of the spool (incoming/ and ready/), by node, and the size limit (dashed). The uploader updates it when it scans the spool: at startup, every 30 s, and when the handler reports a spooled capture.",
    [target(f"sum by (cluster, node, pod) (coredrop_spool_bytes{{{S}}})", "{{cluster}} {{node}} {{pod}}", ref="A"),
     target(f"max by (cluster, node, pod) (coredrop_spool_max_bytes{{{S}}} > 0)", "limit {{cluster}} {{node}} {{pod}}", ref="B")],
    12, y, 6, unit="bytes")
size["fieldConfig"]["overrides"] = [{
    "matcher": {"id": "byFrameRefID", "options": "B"},
    "properties": [{"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [10, 10]}},
                   {"id": "custom.fillOpacity", "value": 0}]}]
panels.append(size)
panels.append(ts(
    "Expired captures",
    "Spooled captures deleted in each interval because they were older than the spool max age. A capture with a readable capture.json is also counted as failed.",
    [target(f"sum by (cluster, node, pod) (increase(coredrop_bundles_expired_total{{{S}}}[$__interval]))", "{{cluster}} {{node}} {{pod}}")],
    18, y, 6, bars=True, interval="1m"))
y += 8
panels.append(ts(
    "Spool captures",
    "Captures in the spool that wait for upload, by node.",
    [target(f"sum by (cluster, node, pod) (coredrop_spool_bundles{{{S}}})", "{{cluster}} {{node}} {{pod}}")],
    0, y, 12))
panels.append(ts(
    "Oldest spooled capture",
    "Age of the oldest capture that waits for upload, by node. It grows while uploads fail.",
    [target(f"max by (cluster, node, pod) (coredrop_spool_oldest_bundle_age_seconds{{{S}}})", "{{cluster}} {{node}} {{pod}}")],
    12, y, 12, unit="s"))
y += 8

# --- Kubernetes Events ------------------------------------------------------
panels.append(row("Kubernetes Events", y)); y += 1
ev = ts("Events posted",
        "Kubernetes Event create and patch requests per minute, by result.",
        [target(f"sum by (result) (rate(coredrop_events_posted_total{{{S}}}[$__rate_interval])) * 60", "{{result}}")],
        0, y, 12)
ev["fieldConfig"]["overrides"] = result_colors()
panels.append(ev)
panels.append(ts(
    "Dropped capture datagrams",
    "Capture datagrams from the handler that the daemon could not parse, in each interval. Expect 0.",
    [target(f"sum by (cluster, node, pod) (increase(coredrop_event_payloads_dropped_total{{{S}}}[$__interval]))", "{{cluster}} {{node}} {{pod}}")],
    12, y, 12, bars=True, interval="1m"))
y += 8

# --- Fleet ------------------------------------------------------------------
panels.append(row("Fleet", y)); y += 1
panels.append(table(
    "Daemons",
    "Each coredrop daemon pod that Prometheus scrapes now, with its version.",
    f"coredrop_build_info{{{S}}}",
    0, y, 24, 8,
    ["Time", "Value", "__name__", "container", "endpoint", "instance", "job"]))


def var(name, label, query):
    return {
        "type": "query", "name": name, "label": label, "datasource": DS,
        "query": {"query": query, "refId": f"{name}-var", "qryType": 1},
        "definition": query, "refresh": 2, "includeAll": True, "multi": True,
        "allValue": ".*", "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
        "sort": 1, "hide": 0, "regex": "", "options": [],
    }


dashboard = {
    "uid": "coredrop",
    "title": "coredrop",
    "description": "Captures, uploads, spool and Kubernetes Events of one or more coredrop deployments.",
    "tags": ["coredrop"],
    "editable": True,
    "graphTooltip": 1,
    "schemaVersion": 39,
    "version": 1,
    "time": {"from": "now-6h", "to": "now"},
    "refresh": "1m",
    "timezone": "",
    "annotations": {"list": []},
    "links": [],
    "templating": {"list": [
        {"type": "datasource", "name": "datasource", "label": "Data source", "query": "prometheus",
         "current": {}, "hide": 0, "refresh": 1, "regex": "", "options": []},
        var("cluster", "Cluster", "label_values(coredrop_build_info, cluster)"),
        var("namespace", "Namespace", 'label_values(coredrop_build_info{cluster=~"$cluster"}, namespace)'),
        var("pod", "Pod", 'label_values(coredrop_build_info{cluster=~"$cluster",namespace=~"$namespace"}, pod)'),
    ]},
    "panels": panels,
}

json.dump(dashboard, sys.stdout, indent=2)
sys.stdout.write("\n")
