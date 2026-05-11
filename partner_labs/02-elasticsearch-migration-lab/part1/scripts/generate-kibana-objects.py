#!/usr/bin/env python3
"""
Generate a valid Kibana 8.15 saved objects NDJSON file for the lab environment.
Uses deterministic UUIDs via uuid.uuid5(uuid.NAMESPACE_DNS, name).
Produces: 5 index-patterns + 30 visualizations + 6 dashboards = 41 objects.
"""

import json
import uuid
import os

UPDATED_AT = "2024-01-01T00:00:00.000Z"
VERSION = "WzEsMV0="
OUTPUT_PATH = "/Users/yss/code/ClickHouse_Demos/partner_labs/02-elasticsearch-migration-lab/part1/dashboards/kibana-dashboards.ndjson"


def make_id(name: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, name))


def make_index_pattern(title: str, fields: list) -> dict:
    return {
        "id": make_id(title),
        "type": "index-pattern",
        "namespaces": ["default"],
        "updated_at": UPDATED_AT,
        "version": VERSION,
        "attributes": {
            "title": title,
            "timeFieldName": "@timestamp",
        },
        "references": [],
        "managed": False,
    }


def make_visualization(
    title: str,
    viz_type: str,
    aggs: list,
    index_pattern_title: str,
    query_filter: str = "",
    value_axis_label: str = "Count",
) -> dict:
    index_pattern_id = make_id(index_pattern_title)
    viz_id = make_id(f"viz-{title}")

    if viz_type == "metric":
        params = {
            "addTooltip": True,
            "addLegend": False,
            "type": "metric",
            "metric": {
                "percentageMode": False,
                "useRanges": False,
                "colorSchema": "Green to Red",
                "metricColorMode": "None",
                "colorsRange": [{"from": 0, "to": 10000}],
                "labels": {"show": True},
                "invertColors": False,
                "style": {"bgFill": "#000", "bgColor": False, "labelColor": False, "subText": "", "fontSize": 60},
            },
        }
    elif viz_type == "pie":
        params = {
            "addTooltip": True,
            "addLegend": True,
            "legendPosition": "right",
            "isDonut": True,
            "labels": {"show": False, "values": True, "last_level": True, "truncate": 100},
        }
    else:
        # line, area, horizontal_bar
        category_axis = {
            "id": "CategoryAxis-1",
            "type": "category",
            "position": "bottom",
            "show": True,
            "style": {},
            "scale": {"type": "linear"},
            "labels": {"show": True, "truncate": 100},
            "title": {},
        }
        value_axis = {
            "id": "ValueAxis-1",
            "name": "LeftAxis-1",
            "type": "value",
            "position": "left",
            "show": True,
            "style": {},
            "scale": {"type": "linear", "mode": "normal"},
            "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
            "title": {"text": value_axis_label},
        }
        series_type = viz_type if viz_type != "horizontal_bar" else "histogram"
        series_params = {
            "show": True,
            "type": series_type,
            "mode": "normal",
            "data": {"label": value_axis_label, "id": "1"},
            "valueAxis": "ValueAxis-1",
            "drawLinesBetweenPoints": True,
            "lineWidth": 2,
            "interpolate": "linear",
            "showCircles": True,
        }
        params = {
            "addLegend": True,
            "addTimeMarker": False,
            "addTooltip": True,
            "categoryAxes": [category_axis],
            "seriesParams": [series_params],
            "valueAxes": [value_axis],
            "grid": {"categoryLines": False},
            "legendPosition": "right",
            "times": [],
            "type": viz_type,
        }

    vis_state = json.dumps({
        "title": title,
        "type": viz_type,
        "params": params,
        "aggs": aggs,
    })

    search_source_json = json.dumps({
        "query": {"query": query_filter, "language": "kuery"},
        "filter": [],
        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
    })

    return {
        "id": viz_id,
        "type": "visualization",
        "namespaces": ["default"],
        "updated_at": UPDATED_AT,
        "version": VERSION,
        "attributes": {
            "title": title,
            "visState": vis_state,
            "uiStateJSON": "{}",
            "description": "",
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": search_source_json,
            },
        },
        "references": [
            {
                "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                "type": "index-pattern",
                "id": index_pattern_id,
            }
        ],
        "managed": False,
    }


def make_dashboard(title: str, description: str, data_view_title: str, panels_config: list) -> dict:
    """panels_config: list of dicts with keys: viz_title, x, y, w, h"""
    panels = []
    references = []

    for i, panel in enumerate(panels_config):
        viz_title = panel["viz_title"]
        viz_id = make_id(f"viz-{viz_title}")
        panel_ref_name = f"panel_{i}"

        panels.append({
            "version": "8.15.0",
            "type": "visualization",
            "gridData": {
                "x": panel["x"],
                "y": panel["y"],
                "w": panel["w"],
                "h": panel["h"],
                "i": str(i),
            },
            "panelIndex": str(i),
            "embeddableConfig": {"enhancements": {}},
            "panelRefName": panel_ref_name,
        })

        references.append({
            "name": panel_ref_name,
            "type": "visualization",
            "id": viz_id,
        })

    options_json = json.dumps({
        "useMargins": True,
        "syncColors": False,
        "syncCursor": True,
        "syncTooltips": False,
        "hidePanelTitles": False,
    })

    search_source_json = json.dumps({
        "query": {"query": "", "language": "kuery"},
        "filter": [],
    })

    return {
        "id": make_id(title),
        "type": "dashboard",
        "namespaces": ["default"],
        "updated_at": UPDATED_AT,
        "version": VERSION,
        "attributes": {
            "title": title,
            "description": description,
            "panelsJSON": json.dumps(panels),
            "optionsJSON": options_json,
            "version": 1,
            "timeRestore": True,
            "timeTo": "now",
            "timeFrom": "now-1h",
            "refreshInterval": {"pause": False, "value": 10000},
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": search_source_json,
            },
        },
        "references": references,
        "managed": False,
    }


def main():
    objects = []

    # --- Data Views (index patterns) ---
    data_views = [
        (
            "logs-web_access-lab",
            [
                "@timestamp", "remote_addr", "request_type", "request_path",
                "status", "size", "user_agent", "run_time", "service",
                "geo.country_name", "geo.city_name", "geo.location",
                "user_agent_parsed.name", "user_agent_parsed.os",
                "user_agent_parsed.device", "event.severity", "log_type",
            ],
        ),
        (
            "logs-application-lab",
            [
                "@timestamp", "level", "service", "message",
                "trace_id", "span_id", "event.severity", "log_type",
            ],
        ),
        (
            "logs-infrastructure-lab",
            [
                "@timestamp", "message", "hostname", "process", "pid",
                "log_message", "event.severity", "syslog_timestamp", "log_type",
            ],
        ),
        (
            "traces-apm-*",
            [
                "@timestamp", "service.name", "service.language.name",
                "transaction.name", "transaction.duration.us", "transaction.type",
                "span.name", "span.duration.us", "trace.id",
                "event.outcome", "processor.event",
                "http.request.method", "http.response.status_code",
            ],
        ),
        (
            "logs-apm.*",
            [
                "@timestamp", "service.name", "service.language.name",
                "message", "trace.id", "span.id",
            ],
        ),
    ]

    for title, fields in data_views:
        objects.append(make_index_pattern(title, fields))

    # =========================================================================
    # Dashboard 1: Web Traffic Overview
    # =========================================================================
    viz1 = make_visualization(
        title="Requests Over Time",
        viz_type="line",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "auto", "min_doc_count": 1}},
        ],
        index_pattern_title="logs-web_access-lab",
    )

    viz2 = make_visualization(
        title="Status Code Distribution",
        viz_type="pie",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "status", "size": 10, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-web_access-lab",
    )

    viz3 = make_visualization(
        title="5xx Error Count",
        viz_type="metric",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
        ],
        index_pattern_title="logs-web_access-lab",
        query_filter="status >= 500",
    )

    viz_web_avg_rt = make_visualization(
        title="Avg Response Time (s)",
        viz_type="metric",
        aggs=[
            {"id": "1", "enabled": True, "type": "avg", "schema": "metric",
             "params": {"field": "run_time"}},
        ],
        index_pattern_title="logs-web_access-lab",
    )

    viz_web_top_paths = make_visualization(
        title="Top Request Paths",
        viz_type="horizontal_bar",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "request_path", "size": 10, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-web_access-lab",
    )

    viz_web_top_countries = make_visualization(
        title="Top Countries",
        viz_type="horizontal_bar",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "geo.country_name", "size": 10, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-web_access-lab",
    )

    viz_web_method_dist = make_visualization(
        title="HTTP Method Distribution",
        viz_type="pie",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "request_type", "size": 5, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-web_access-lab",
    )

    viz_web_top_ua = make_visualization(
        title="Top User Agents",
        viz_type="horizontal_bar",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "user_agent_parsed.name", "size": 10, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-web_access-lab",
    )

    # =========================================================================
    # Dashboard 2: Application Health
    # =========================================================================
    viz4 = make_visualization(
        title="Log Volume by Severity",
        viz_type="area",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "auto"}},
            {"id": "3", "enabled": True, "type": "terms", "schema": "group",
             "params": {"field": "event.severity", "size": 5, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-application-lab",
    )

    viz5 = make_visualization(
        title="Error Count",
        viz_type="metric",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
        ],
        index_pattern_title="logs-application-lab",
        query_filter="level:ERROR",
    )

    viz_app_level_dist = make_visualization(
        title="Log Level Distribution",
        viz_type="pie",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "level", "size": 5, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-application-lab",
    )

    viz_app_errors_by_svc = make_visualization(
        title="Errors by Service",
        viz_type="horizontal_bar",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "service", "size": 10, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-application-lab",
        query_filter="level:ERROR",
    )

    viz_app_errors_over_time = make_visualization(
        title="Error Volume Over Time",
        viz_type="line",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "auto", "min_doc_count": 1}},
        ],
        index_pattern_title="logs-application-lab",
        query_filter="level:ERROR",
    )

    # =========================================================================
    # Dashboard 3: Infrastructure Overview
    # =========================================================================
    viz6 = make_visualization(
        title="Syslog Volume by Host",
        viz_type="area",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "auto"}},
            {"id": "3", "enabled": True, "type": "terms", "schema": "group",
             "params": {"field": "hostname", "size": 10, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-infrastructure-lab",
    )

    viz7 = make_visualization(
        title="Top Processes",
        viz_type="horizontal_bar",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "process", "size": 10, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-infrastructure-lab",
    )

    viz_infra_severity_dist = make_visualization(
        title="Severity Distribution",
        viz_type="pie",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "event.severity", "size": 5, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-infrastructure-lab",
    )

    viz_infra_by_process = make_visualization(
        title="Log Volume by Process",
        viz_type="area",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "auto"}},
            {"id": "3", "enabled": True, "type": "terms", "schema": "group",
             "params": {"field": "process", "size": 10, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-infrastructure-lab",
    )

    # =========================================================================
    # Dashboard 4: OTel Demo — APM Traces  (traces-apm-*)
    # =========================================================================
    viz8 = make_visualization(
        title="APM Trace Volume",
        viz_type="line",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "auto", "min_doc_count": 1}},
        ],
        index_pattern_title="traces-apm-*",
    )

    viz9 = make_visualization(
        title="Top Services by Span Count",
        viz_type="horizontal_bar",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "service.name", "size": 15, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="traces-apm-*",
    )

    viz_apm_top_txns = make_visualization(
        title="Top Transaction Names",
        viz_type="horizontal_bar",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "transaction.name", "size": 15, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="traces-apm-*",
        query_filter="processor.event:transaction",
    )

    viz_apm_outcome_dist = make_visualization(
        title="Trace Outcome Distribution",
        viz_type="pie",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "event.outcome", "size": 5, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="traces-apm-*",
    )

    viz_apm_http_status = make_visualization(
        title="HTTP Status Codes",
        viz_type="pie",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "http.response.status_code", "size": 10, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="traces-apm-*",
        query_filter="_exists_:http.response.status_code",
    )

    viz_apm_languages = make_visualization(
        title="Service Language Breakdown",
        viz_type="pie",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "service.language.name", "size": 10, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="traces-apm-*",
    )

    # =========================================================================
    # Dashboard 5: OTel Demo — Latency  (traces-apm-*, transactions only)
    # =========================================================================
    viz_lat_over_time = make_visualization(
        title="Avg Transaction Duration Over Time",
        viz_type="line",
        aggs=[
            {"id": "1", "enabled": True, "type": "avg", "schema": "metric",
             "params": {"field": "transaction.duration.us"}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "auto", "min_doc_count": 1}},
        ],
        index_pattern_title="traces-apm-*",
        query_filter="processor.event:transaction",
        value_axis_label="Avg Duration (µs)",
    )

    viz_lat_by_svc = make_visualization(
        title="Avg Duration by Service",
        viz_type="horizontal_bar",
        aggs=[
            {"id": "1", "enabled": True, "type": "avg", "schema": "metric",
             "params": {"field": "transaction.duration.us"}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "service.name", "size": 15, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="traces-apm-*",
        query_filter="processor.event:transaction",
        value_axis_label="Avg Duration (µs)",
    )

    viz_err_over_time = make_visualization(
        title="Failed Transactions Over Time",
        viz_type="line",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "auto", "min_doc_count": 1}},
        ],
        index_pattern_title="traces-apm-*",
        query_filter="event.outcome:failure",
    )

    viz_err_by_svc = make_visualization(
        title="Failed Transactions by Service",
        viz_type="horizontal_bar",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "service.name", "size": 15, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="traces-apm-*",
        query_filter="event.outcome:failure",
    )

    # =========================================================================
    # Dashboard 6: OTel Demo — Logs  (logs-apm.*)
    # =========================================================================
    viz_log_vol_by_svc = make_visualization(
        title="OTel Log Volume by Service",
        viz_type="area",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
             "params": {"field": "@timestamp", "interval": "auto"}},
            {"id": "3", "enabled": True, "type": "terms", "schema": "group",
             "params": {"field": "service.name", "size": 15, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-apm.*",
    )

    viz_log_top_svcs = make_visualization(
        title="Top Services by Log Volume",
        viz_type="horizontal_bar",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "service.name", "size": 15, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-apm.*",
    )

    viz_log_by_lang = make_visualization(
        title="OTel Logs by Service Language",
        viz_type="pie",
        aggs=[
            {"id": "1", "enabled": True, "type": "count", "schema": "metric", "params": {}},
            {"id": "2", "enabled": True, "type": "terms", "schema": "segment",
             "params": {"field": "service.language.name", "size": 10, "order": "desc", "orderBy": "1"}},
        ],
        index_pattern_title="logs-apm.*",
    )

    objects.extend([
        # Web Traffic
        viz1, viz2, viz3, viz_web_avg_rt, viz_web_top_paths, viz_web_top_countries,
        viz_web_method_dist, viz_web_top_ua,
        # Application Health
        viz4, viz5, viz_app_level_dist, viz_app_errors_by_svc, viz_app_errors_over_time,
        # Infrastructure
        viz6, viz7, viz_infra_severity_dist, viz_infra_by_process,
        # OTel Traces
        viz8, viz9, viz_apm_top_txns, viz_apm_outcome_dist, viz_apm_http_status, viz_apm_languages,
        # OTel Latency
        viz_lat_over_time, viz_lat_by_svc, viz_err_over_time, viz_err_by_svc,
        # OTel Logs
        viz_log_vol_by_svc, viz_log_top_svcs, viz_log_by_lang,
    ])

    # =========================================================================
    # Dashboards
    # =========================================================================
    dashboards = [
        (
            "Web Traffic Overview",
            "Web access logs: request rates, status codes, response time, geographic breakdown, top paths",
            "logs-web_access-lab",
            [
                {"viz_title": "Requests Over Time",        "x": 0,  "y": 0,  "w": 48, "h": 15},
                {"viz_title": "Status Code Distribution",  "x": 0,  "y": 15, "w": 16, "h": 15},
                {"viz_title": "5xx Error Count",           "x": 16, "y": 15, "w": 16, "h": 15},
                {"viz_title": "Avg Response Time (s)",     "x": 32, "y": 15, "w": 16, "h": 15},
                {"viz_title": "Top Request Paths",         "x": 0,  "y": 30, "w": 24, "h": 15},
                {"viz_title": "Top Countries",             "x": 24, "y": 30, "w": 24, "h": 15},
                {"viz_title": "HTTP Method Distribution",  "x": 0,  "y": 45, "w": 16, "h": 15},
                {"viz_title": "Top User Agents",           "x": 16, "y": 45, "w": 32, "h": 15},
            ],
        ),
        (
            "Application Health",
            "Application log volume by severity, error trends, level distribution, errors by service",
            "logs-application-lab",
            [
                {"viz_title": "Log Volume by Severity",    "x": 0,  "y": 0,  "w": 48, "h": 15},
                {"viz_title": "Log Level Distribution",    "x": 0,  "y": 15, "w": 16, "h": 15},
                {"viz_title": "Error Count",               "x": 16, "y": 15, "w": 16, "h": 15},
                {"viz_title": "Errors by Service",         "x": 32, "y": 15, "w": 16, "h": 15},
                {"viz_title": "Error Volume Over Time",    "x": 0,  "y": 30, "w": 48, "h": 15},
            ],
        ),
        (
            "Infrastructure Overview",
            "Syslog volume by host, top processes, severity distribution, log volume by process",
            "logs-infrastructure-lab",
            [
                {"viz_title": "Syslog Volume by Host",     "x": 0,  "y": 0,  "w": 48, "h": 15},
                {"viz_title": "Top Processes",             "x": 0,  "y": 15, "w": 24, "h": 15},
                {"viz_title": "Severity Distribution",     "x": 24, "y": 15, "w": 24, "h": 15},
                {"viz_title": "Log Volume by Process",     "x": 0,  "y": 30, "w": 48, "h": 15},
            ],
        ),
        (
            "OTel Demo — APM Traces",
            "OTel Demo: trace volume, top services and transactions, outcome distribution, HTTP status codes, language breakdown",
            "traces-apm-*",
            [
                {"viz_title": "APM Trace Volume",             "x": 0,  "y": 0,  "w": 48, "h": 15},
                {"viz_title": "Trace Outcome Distribution",   "x": 0,  "y": 15, "w": 16, "h": 15},
                {"viz_title": "HTTP Status Codes",            "x": 16, "y": 15, "w": 16, "h": 15},
                {"viz_title": "Service Language Breakdown",   "x": 32, "y": 15, "w": 16, "h": 15},
                {"viz_title": "Top Services by Span Count",   "x": 0,  "y": 30, "w": 24, "h": 15},
                {"viz_title": "Top Transaction Names",        "x": 24, "y": 30, "w": 24, "h": 15},
            ],
        ),
        (
            "OTel Demo — Latency",
            "OTel Demo: transaction latency trends, avg duration per service, failure rates and failed transactions by service",
            "traces-apm-*",
            [
                {"viz_title": "Avg Transaction Duration Over Time", "x": 0,  "y": 0,  "w": 48, "h": 15},
                {"viz_title": "Avg Duration by Service",            "x": 0,  "y": 15, "w": 24, "h": 15},
                {"viz_title": "Failed Transactions by Service",     "x": 24, "y": 15, "w": 24, "h": 15},
                {"viz_title": "Failed Transactions Over Time",      "x": 0,  "y": 30, "w": 48, "h": 15},
            ],
        ),
        (
            "OTel Demo — Logs",
            "OTel Demo: structured log volume by service over time, top services by log count, language distribution",
            "logs-apm.*",
            [
                {"viz_title": "OTel Log Volume by Service",   "x": 0,  "y": 0,  "w": 48, "h": 15},
                {"viz_title": "Top Services by Log Volume",   "x": 0,  "y": 15, "w": 24, "h": 15},
                {"viz_title": "OTel Logs by Service Language","x": 24, "y": 15, "w": 24, "h": 15},
            ],
        ),
    ]

    for title, description, data_view_title, panels_config in dashboards:
        objects.append(make_dashboard(title, description, data_view_title, panels_config))

    # Write NDJSON — one JSON object per line, no trailing blank line
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for obj in objects:
            f.write(json.dumps(obj, separators=(",", ":")) + "\n")

    n_viz = sum(1 for o in objects if o["type"] == "visualization")
    n_ip  = sum(1 for o in objects if o["type"] == "index-pattern")
    n_db  = sum(1 for o in objects if o["type"] == "dashboard")
    print(f"Written {len(objects)} objects to {OUTPUT_PATH}")
    print(f"  index-patterns : {n_ip}")
    print(f"  visualizations : {n_viz}")
    print(f"  dashboards     : {n_db}")
    print(f"  total          : {len(objects)}")


if __name__ == "__main__":
    main()
