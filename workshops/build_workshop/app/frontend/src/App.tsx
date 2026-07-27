import { NavLink, Navigate, Route, Routes } from "react-router-dom";

import { DashboardPage } from "./pages/DashboardPage";
import { HistoricalPage } from "./pages/HistoricalPage";

export function App() {
  return (
    <div className="app-shell">
      <header className="app-header">
        <div className="brand">
          {/* Official ClickHouse mark (Click UI Clickhouse.tsx): four rounded bars + one
              short bar, monochrome. currentColor renders it white on the dark header. */}
          <svg className="brand-logo" viewBox="0 0 54 54" fill="none" role="img" aria-label="ClickHouse">
            <rect width="5.9998" height="53.9982" rx="1.45943" fill="currentColor" />
            <rect x="12" width="5.9998" height="53.9982" rx="1.45943" fill="currentColor" />
            <rect x="24.001" width="5.9998" height="53.9982" rx="1.45943" fill="currentColor" />
            <rect x="35.998" width="5.9998" height="53.9982" rx="1.45943" fill="currentColor" />
            <rect x="48.001" y="21.0005" width="5.9998" height="11.9996" rx="1.45943" fill="currentColor" />
          </svg>
          <span className="brand-name">ClickHouse</span>
          <span className="brand-sep" aria-hidden="true">/</span>
          <span className="app-name">NYC Taxi Ops</span>
          <span className="app-badge">BUILD Workshop</span>
        </div>

        <nav className="app-nav">
          <NavLink to="/" end className="tab">
            <span className="live-dot" aria-hidden="true" />
            Ops · Live
          </NavLink>
          <NavLink to="/historical" className="tab">
            Historical
          </NavLink>
          <a className="ghost-link" href="/api/docs" target="_blank" rel="noreferrer">
            API Docs ↗
          </a>
        </nav>
      </header>

      <main className="app-main pt-3">
        <Routes>
          <Route path="/" element={<DashboardPage />} />
          <Route path="/historical" element={<HistoricalPage />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </div>
  );
}

