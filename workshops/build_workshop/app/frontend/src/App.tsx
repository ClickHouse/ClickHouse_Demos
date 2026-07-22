import { Link, Navigate, Route, Routes } from "react-router-dom";

import { DashboardPage } from "./pages/DashboardPage";
import { HistoricalPage } from "./pages/HistoricalPage";

export function App() {
  return (
    <div className="container-fluid py-3">
      <div className="d-flex align-items-center justify-content-between mb-2">
        <div>
          <div className="h3 mb-0">NYC Taxi Ops War Room</div>
          <div className="text-secondary">ClickHouse BUILD Workshop (ClickHouse + FastAPI + React)</div>
        </div>
        <div className="d-flex gap-2">
          <a className="btn btn-outline-secondary btn-sm" href="/api/docs" target="_blank" rel="noreferrer">
            API Docs
          </a>
        </div>
      </div>

      <ul className="nav nav-pills mb-3">
        <li className="nav-item">
          <Link className="nav-link" to="/">
            Ops (Live)
          </Link>
        </li>
        <li className="nav-item">
          <Link className="nav-link" to="/historical">
            Historical
          </Link>
        </li>
      </ul>

      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/historical" element={<HistoricalPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </div>
  );
}

