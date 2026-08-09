#!/bin/bash
set -euo pipefail
ES_URL="${ES_URL:-http://elasticsearch:9200}"
KIBANA_URL="${KIBANA_URL:-http://kibana:5601}"

echo "Waiting for Elasticsearch..."
until curl -sf "${ES_URL}/_cluster/health?wait_for_status=yellow&timeout=30s" > /dev/null; do
  echo "ES not ready, retrying..."
  sleep 5
done
echo "Elasticsearch is ready."

# ─────────────────────────────────────────────
# 1. ILM Policy
# ─────────────────────────────────────────────
echo "Creating ILM policy: lab-observability-policy..."
curl -sf -X PUT "${ES_URL}/_ilm/policy/lab-observability-policy" \
  -H "Content-Type: application/json" \
  -d '{
    "policy": {
      "phases": {
        "hot": {
          "actions": {
            "rollover": {
              "max_size": "5gb",
              "max_age": "1d"
            },
            "set_priority": {
              "priority": 100
            }
          }
        },
        "warm": {
          "min_age": "2d",
          "actions": {
            "shrink": {
              "number_of_shards": 1
            },
            "forcemerge": {
              "max_num_segments": 1
            },
            "set_priority": {
              "priority": 50
            }
          }
        },
        "delete": {
          "min_age": "30d",
          "actions": {
            "delete": {}
          }
        }
      }
    }
  }'
echo "ILM policy created."

# ─────────────────────────────────────────────
# 2. Component Templates
# ─────────────────────────────────────────────

echo "Creating component template: lab-logs-settings..."
curl -sf -X PUT "${ES_URL}/_component_template/lab-logs-settings" \
  -H "Content-Type: application/json" \
  -d '{
    "template": {
      "settings": {
        "number_of_shards": 2,
        "number_of_replicas": 1,
        "index.lifecycle.name": "lab-observability-policy",
        "index.default_pipeline": "default-enrichment"
      }
    }
  }'
echo "Component template lab-logs-settings created."

echo "Creating component template: lab-web-access-mappings..."
curl -sf -X PUT "${ES_URL}/_component_template/lab-web-access-mappings" \
  -H "Content-Type: application/json" \
  -d '{
    "template": {
      "mappings": {
        "properties": {
          "@timestamp":     { "type": "date" },
          "remote_addr":    { "type": "ip" },
          "request_type":   { "type": "keyword" },
          "request_path": {
            "type": "text",
            "fields": {
              "keyword": { "type": "keyword", "ignore_above": 1024 }
            }
          },
          "status":         { "type": "keyword" },
          "size":           { "type": "long" },
          "user_agent": {
            "type": "text",
            "fields": {
              "keyword": { "type": "keyword", "ignore_above": 512 }
            }
          },
          "referer":        { "type": "keyword" },
          "run_time":       { "type": "float" },
          "service":        { "type": "keyword" },
          "geo": {
            "properties": {
              "country_name": { "type": "keyword" },
              "city_name":    { "type": "keyword" },
              "location":     { "type": "geo_point" }
            }
          },
          "user_agent_parsed": {
            "properties": {
              "name":    { "type": "keyword" },
              "version": { "type": "keyword" },
              "os": {
                "properties": {
                  "name":    { "type": "keyword" },
                  "full":    { "type": "keyword" },
                  "version": { "type": "keyword" }
                }
              },
              "device": {
                "properties": {
                  "name": { "type": "keyword" }
                }
              }
            }
          }
        }
      }
    }
  }'
echo "Component template lab-web-access-mappings created."

echo "Creating component template: lab-application-mappings..."
curl -sf -X PUT "${ES_URL}/_component_template/lab-application-mappings" \
  -H "Content-Type: application/json" \
  -d '{
    "template": {
      "mappings": {
        "properties": {
          "@timestamp":      { "type": "date" },
          "level":           { "type": "keyword" },
          "service":         { "type": "keyword" },
          "message": {
            "type": "text",
            "fields": {
              "keyword": { "type": "keyword", "ignore_above": 1024 }
            }
          },
          "trace_id":        { "type": "keyword" },
          "span_id":         { "type": "keyword" },
          "event": {
            "properties": {
              "severity": { "type": "keyword" }
            }
          },
          "log_type":        { "type": "keyword" }
        }
      }
    }
  }'
echo "Component template lab-application-mappings created."

echo "Creating component template: lab-infrastructure-mappings..."
curl -sf -X PUT "${ES_URL}/_component_template/lab-infrastructure-mappings" \
  -H "Content-Type: application/json" \
  -d '{
    "template": {
      "mappings": {
        "properties": {
          "@timestamp":       { "type": "date" },
          "message":          { "type": "text" },
          "hostname":         { "type": "keyword" },
          "process":          { "type": "keyword" },
          "pid":              { "type": "keyword" },
          "log_message":      { "type": "text" },
          "event": {
            "properties": {
              "severity": { "type": "keyword" }
            }
          },
          "syslog_timestamp": { "type": "keyword" },
          "log_type":         { "type": "keyword" }
        }
      }
    }
  }'
echo "Component template lab-infrastructure-mappings created."

# ─────────────────────────────────────────────
# 3. Index Templates
# ─────────────────────────────────────────────

echo "Creating index template: lab-web-access..."
curl -sf -X PUT "${ES_URL}/_index_template/lab-web-access" \
  -H "Content-Type: application/json" \
  -d '{
    "index_patterns": ["logs-web_access-*"],
    "composed_of": ["lab-logs-settings", "lab-web-access-mappings"],
    "priority": 200,
    "data_stream": {}
  }'
echo "Index template lab-web-access created."

echo "Creating index template: lab-application..."
curl -sf -X PUT "${ES_URL}/_index_template/lab-application" \
  -H "Content-Type: application/json" \
  -d '{
    "index_patterns": ["logs-application-*"],
    "composed_of": ["lab-logs-settings", "lab-application-mappings"],
    "priority": 200,
    "data_stream": {}
  }'
echo "Index template lab-application created."

echo "Creating index template: lab-infrastructure..."
curl -sf -X PUT "${ES_URL}/_index_template/lab-infrastructure" \
  -H "Content-Type: application/json" \
  -d '{
    "index_patterns": ["logs-infrastructure-*"],
    "composed_of": ["lab-logs-settings", "lab-infrastructure-mappings"],
    "priority": 200,
    "data_stream": {}
  }'
echo "Index template lab-infrastructure created."

# ─────────────────────────────────────────────
# 4. Ingest Pipelines
# ─────────────────────────────────────────────

echo "Creating ingest pipeline: default-enrichment..."
curl -sf -X PUT "${ES_URL}/_ingest/pipeline/default-enrichment" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Sets event.ingested timestamp on every document",
    "processors": [
      {
        "set": {
          "field": "event.ingested",
          "value": "{{{_ingest.timestamp}}}"
        }
      }
    ]
  }'
echo "Pipeline default-enrichment created."

echo "Creating ingest pipeline: web-access-enrichment..."
curl -sf -X PUT "${ES_URL}/_ingest/pipeline/web-access-enrichment" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Enrich web access logs with geo, user agent info, and severity",
    "processors": [
      {
        "geoip": {
          "field": "remote_addr",
          "target_field": "geo",
          "ignore_missing": true,
          "ignore_failure": true
        }
      },
      {
        "user_agent": {
          "field": "user_agent",
          "target_field": "user_agent_parsed",
          "ignore_missing": true,
          "ignore_failure": true
        }
      },
      {
        "set": {
          "field": "event.severity",
          "value": "info"
        }
      },
      {
        "script": {
          "description": "Set event severity based on HTTP status code",
          "lang": "painless",
          "source": "def status = ctx.status; if (status != null) { int code = Integer.parseInt(status); if (code >= 500) { ctx[\"event\"][\"severity\"] = \"error\"; } else if (code >= 400) { ctx[\"event\"][\"severity\"] = \"warn\"; } else { ctx[\"event\"][\"severity\"] = \"info\"; } }",
          "ignore_failure": true
        }
      }
    ]
  }'
echo "Pipeline web-access-enrichment created."

echo "Creating ingest pipeline: app-log-enrichment..."
curl -sf -X PUT "${ES_URL}/_ingest/pipeline/app-log-enrichment" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Enrich application logs with severity and dissected fields",
    "processors": [
      {
        "set": {
          "field": "event.severity",
          "copy_from": "level",
          "ignore_empty_value": true
        }
      },
      {
        "lowercase": {
          "field": "event.severity",
          "ignore_missing": true
        }
      },
      {
        "dissect": {
          "field": "message",
          "pattern": "%{_tmp.timestamp} %{_tmp.level} %{_tmp.service} - %{_tmp.body}",
          "ignore_failure": true,
          "ignore_missing": true
        }
      },
      {
        "remove": {
          "field": "_tmp",
          "ignore_missing": true
        }
      }
    ]
  }'
echo "Pipeline app-log-enrichment created."

echo "Creating ingest pipeline: infra-log-parsing..."
curl -sf -X PUT "${ES_URL}/_ingest/pipeline/infra-log-parsing" \
  -H "Content-Type: application/json" \
  -d '{
    "description": "Parse infrastructure/syslog format logs and set severity",
    "processors": [
      {
        "grok": {
          "field": "message",
          "patterns": [
            "%{SYSLOGTIMESTAMP:syslog_timestamp} %{HOSTNAME:hostname} %{WORD:process}(?:\\[%{POSINT:pid}\\])?: %{GREEDYDATA:log_message}"
          ],
          "ignore_failure": true,
          "ignore_missing": true
        }
      },
      {
        "set": {
          "field": "event.severity",
          "value": "info"
        }
      },
      {
        "script": {
          "description": "Set event severity based on log message content",
          "lang": "painless",
          "source": "def msg = ctx.log_message != null ? ctx.log_message.toLowerCase() : (ctx.message != null ? ctx.message.toLowerCase() : \"\"); if (msg.contains(\"error\") || msg.contains(\"critical\") || msg.contains(\"fatal\")) { ctx[\"event\"][\"severity\"] = \"error\"; } else if (msg.contains(\"warn\") || msg.contains(\"warning\")) { ctx[\"event\"][\"severity\"] = \"warn\"; }",
          "ignore_failure": true
        }
      }
    ]
  }'
echo "Pipeline infra-log-parsing created."

# ─────────────────────────────────────────────
# 5. Import Kibana Dashboards
# ─────────────────────────────────────────────

echo "Waiting for Kibana..."
until curl -sf "${KIBANA_URL}/api/status" -H "kbn-xsrf: true" > /dev/null; do
  echo "Kibana not ready, retrying..."
  sleep 5
done
echo "Kibana is ready. Importing dashboards..."
curl -X POST "${KIBANA_URL}/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  -F "file=@/dashboards/kibana-dashboards.ndjson"
echo "Dashboards imported."

# ─────────────────────────────────────────────
# 6. Kibana Data Views for APM indices
# ─────────────────────────────────────────────
echo "Creating Kibana data views for APM indices..."

for pattern in "traces-apm-*" "logs-apm.*" "metrics-apm.*"; do
  # strip trailing * or . for the ID
  view_id=$(echo "$pattern" | tr -d '*.' | tr '_' '-')
  curl -sf -X POST "${KIBANA_URL}/api/data_views/data_view" \
    -H "kbn-xsrf: true" \
    -H "Content-Type: application/json" \
    -d "{
      \"data_view\": {
        \"title\": \"${pattern}\",
        \"timeFieldName\": \"@timestamp\",
        \"id\": \"${view_id}\"
      }
    }" > /dev/null || true
  echo "Data view '${pattern}' created (or already exists)."
done

echo "Bootstrap complete!"
