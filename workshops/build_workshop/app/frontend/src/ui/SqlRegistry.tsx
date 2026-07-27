import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from "react";

import { SqlPopover } from "./SqlPopover";

// Panels fetch their own data (and thus own the executed SQL), but the "Show SQL"
// affordance lives next to each panel's title up in the page header. This tiny
// registry bridges that gap: a panel reports its current SQL under a stable key,
// and the title renders a <PanelSqlButton> that reads it back.
//
// Two separate contexts so panels (which only report) subscribe to a stable
// dispatch value and never re-render when some other panel's SQL changes; only
// the title buttons (which read) re-render on updates.
const ReportContext = createContext<(key: string, sql: string | null | undefined) => void>(() => {});
const SqlsContext = createContext<Record<string, string | null | undefined>>({});

export function SqlRegistryProvider({ children }: { children: ReactNode }) {
  const [sqls, setSqls] = useState<Record<string, string | null | undefined>>({});
  const report = useCallback((key: string, sql: string | null | undefined) => {
    // No-op when unchanged so steady-state refetches don't trigger re-renders.
    setSqls((prev) => (prev[key] === sql ? prev : { ...prev, [key]: sql }));
  }, []);
  return (
    <ReportContext.Provider value={report}>
      <SqlsContext.Provider value={sqls}>{children}</SqlsContext.Provider>
    </ReportContext.Provider>
  );
}

// Called by a panel to publish the SQL behind its current data.
export function useReportSql(key: string, sql: string | null | undefined) {
  const report = useContext(ReportContext);
  useEffect(() => {
    report(key, sql);
  }, [report, key, sql]);
}

// The title-adjacent trigger: reads the reported SQL for `sqlKey` and renders the
// popover. Hidden until the panel has reported a query (SqlPopover returns null).
export function PanelSqlButton({ sqlKey }: { sqlKey: string }) {
  const sql = useContext(SqlsContext)[sqlKey];
  return <SqlPopover sql={sql} align="start" />;
}
