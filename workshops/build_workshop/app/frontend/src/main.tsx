import "bootstrap/dist/css/bootstrap.min.css";
import "maplibre-gl/dist/maplibre-gl.css";

import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter } from "react-router-dom";
import { ErrorBoundary, type FallbackProps } from "react-error-boundary";

import { App } from "./App";
import { attachErrorBoundary, initObservability } from "./observability";

// Start browser observability, then report React render errors caught by the
// error boundary below. Both are no-ops unless the app was built with the
// ClickStack (module 05) config; see observability.ts.
initObservability();
attachErrorBoundary(ErrorBoundary);

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false
    }
  }
});

function ErrorFallback({ error }: FallbackProps) {
  const message = error instanceof Error ? error.message : String(error);
  return (
    <div className="container-fluid py-4">
      <div className="alert alert-danger" role="alert">
        <div className="fw-bold">Something went wrong.</div>
        <div className="small mt-1">{message}</div>
      </div>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ErrorBoundary FallbackComponent={ErrorFallback}>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </QueryClientProvider>
    </ErrorBoundary>
  </React.StrictMode>
);
