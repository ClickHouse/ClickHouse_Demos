import { useMemo, useRef, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import type { EChartsOption } from "echarts";

import { api } from "../api/client";
import type { ChatChartSpec, ChatResponse } from "../api/types";
import { EChart } from "./EChart";

type UserEntry = { role: "user"; text: string };
type AssistantEntry = { role: "assistant" } & ChatResponse;
type ErrorEntry = { role: "error"; text: string };
type ChatEntry = UserEntry | AssistantEntry | ErrorEntry;

// Group all turns from one panel session under a single Langfuse session id.
function newConversationId() {
  return (globalThis.crypto?.randomUUID?.() ?? `chat-${Date.now()}-${Math.random().toString(16).slice(2)}`);
}

function formatCell(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "number") return Number.isInteger(v) ? v.toLocaleString() : v.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return String(v);
}

function buildChartOption(chart: ChatChartSpec, rows: Record<string, unknown>[]): EChartsOption | null {
  if (chart.type === "none" || !chart.x || !chart.y || rows.length === 0) return null;
  const yKeys = Array.isArray(chart.y) ? chart.y : [chart.y];
  const xKey = chart.x;
  const type = chart.type === "bar" ? "bar" : "line";

  return {
    tooltip: { trigger: "axis" },
    legend: { top: 0 },
    grid: { left: 55, right: 20, top: 30, bottom: 60 },
    xAxis: { type: "category", data: rows.map((r) => formatCell(r[xKey])), axisLabel: { rotate: 45, hideOverlap: true } },
    yAxis: { type: "value" },
    series: yKeys.map((k) => ({
      name: k,
      type,
      showSymbol: false,
      data: rows.map((r) => Number(r[k] ?? 0))
    }))
  };
}

function RowsTable({ rows }: { rows: Record<string, unknown>[] }) {
  const cols = Object.keys(rows[0] ?? {});
  const shown = rows.slice(0, 8);
  return (
    <div className="table-responsive mt-2" style={{ maxHeight: 220 }}>
      <table className="table table-sm table-hover align-middle mb-1">
        <thead>
          <tr>
            {cols.map((c) => (
              <th key={c}>{c}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {shown.map((r, i) => (
            <tr key={i}>
              {cols.map((c) => (
                <td key={c}>{formatCell(r[c])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > shown.length && (
        <div className="text-secondary small">Showing {shown.length} of {rows.length} rows.</div>
      )}
    </div>
  );
}

function AssistantMessage({ entry }: { entry: AssistantEntry }) {
  const [showSql, setShowSql] = useState(false);
  const chartOption = useMemo(
    () => (entry.chart && entry.rows ? buildChartOption(entry.chart, entry.rows) : null),
    [entry.chart, entry.rows]
  );

  return (
    <div className="d-flex mb-2">
      <div className="bg-light border rounded p-2 w-100">
        <div>{entry.answer}</div>

        {entry.sql && (
          <div className="mt-2">
            <button className="btn btn-outline-secondary btn-sm py-0" onClick={() => setShowSql((v) => !v)}>
              {showSql ? "Hide SQL" : "Show SQL"}
            </button>
            {showSql && (
              <pre className="bg-dark text-light rounded p-2 mt-2 mb-0 small" style={{ whiteSpace: "pre-wrap" }}>
                {entry.sql}
              </pre>
            )}
          </div>
        )}

        {chartOption && <EChart option={chartOption} height={240} />}
        {entry.rows && entry.rows.length > 0 && <RowsTable rows={entry.rows} />}
      </div>
    </div>
  );
}

export function ChatPanel() {
  const [open, setOpen] = useState(false);
  const [input, setInput] = useState("");
  const [entries, setEntries] = useState<ChatEntry[]>([]);
  const conversationId = useRef<string>(newConversationId());
  const bodyRef = useRef<HTMLDivElement | null>(null);

  const scrollToBottom = () => {
    requestAnimationFrame(() => {
      if (bodyRef.current) bodyRef.current.scrollTop = bodyRef.current.scrollHeight;
    });
  };

  const mutation = useMutation({
    mutationFn: (message: string) => api.chat({ message, conversation_id: conversationId.current }),
    onSuccess: (res) => {
      setEntries((prev) => [...prev, { role: "assistant", ...res }]);
      scrollToBottom();
    },
    onError: (err: Error) => {
      setEntries((prev) => [...prev, { role: "error", text: err.message }]);
      scrollToBottom();
    }
  });

  const send = () => {
    const message = input.trim();
    if (!message || mutation.isPending) return;
    setEntries((prev) => [...prev, { role: "user", text: message }]);
    setInput("");
    mutation.mutate(message);
    scrollToBottom();
  };

  if (!open) {
    return (
      <button
        className="btn btn-primary shadow"
        style={{ position: "fixed", right: 20, bottom: 20, zIndex: 1050, borderRadius: 24 }}
        onClick={() => setOpen(true)}
      >
        Ask AI
      </button>
    );
  }

  return (
    <div
      className="card shadow"
      style={{ position: "fixed", right: 20, bottom: 20, zIndex: 1050, width: 440, maxWidth: "calc(100vw - 40px)", maxHeight: "78vh" }}
    >
      <div className="card-header d-flex justify-content-between align-items-center py-2">
        <div className="fw-semibold">Ask the data (AI)</div>
        <button className="btn-close" aria-label="Close" onClick={() => setOpen(false)} />
      </div>

      <div ref={bodyRef} className="card-body overflow-auto" style={{ minHeight: 200 }}>
        {entries.length === 0 && (
          <div className="text-secondary small">
            Ask a question about the taxi data, e.g. "Top 10 pickup zones by trips in July 2022" or
            "p95 trip duration by borough". The generated SQL is shown with each answer.
          </div>
        )}

        {entries.map((e, i) => {
          if (e.role === "user") {
            return (
              <div key={i} className="d-flex justify-content-end mb-2">
                <div className="bg-primary text-white rounded p-2" style={{ maxWidth: "85%" }}>
                  {e.text}
                </div>
              </div>
            );
          }
          if (e.role === "error") {
            return (
              <div key={i} className="alert alert-warning py-2 px-2 small mb-2" role="alert">
                {e.text}
              </div>
            );
          }
          return <AssistantMessage key={i} entry={e} />;
        })}

        {mutation.isPending && <div className="text-secondary small">Thinking…</div>}
      </div>

      <div className="card-footer py-2">
        <div className="input-group">
          <input
            className="form-control"
            placeholder="Ask about trips, zones, fares…"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") send();
            }}
            disabled={mutation.isPending}
          />
          <button className="btn btn-primary" onClick={send} disabled={mutation.isPending || !input.trim()}>
            Send
          </button>
        </div>
      </div>
    </div>
  );
}
