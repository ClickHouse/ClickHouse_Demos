// Browser observability: send frontend UI errors, console, network activity, and
// session replay to the ClickStack collector (module 05), correlated with the
// backend's traces.
//
// Gated, off by default: initObservability() and attachErrorBoundary() are no-ops
// unless the app was BUILT with VITE_HYPERDX_URL set. Those build args are supplied
// only by docker-compose.otel.yml (module 05), so modules 01-04 run with no browser
// telemetry. Because Vite inlines import.meta.env.* at build time, enabling this
// requires rebuilding the frontend image (`up -d --build`), not just a restart.
import HyperDX from "@hyperdx/browser";
import type { ComponentType } from "react";

const HYPERDX_URL = import.meta.env.VITE_HYPERDX_URL as string | undefined;

// True only when a collector URL was baked in at build time.
export const observabilityEnabled = Boolean(HYPERDX_URL);

export function initObservability(): void {
  if (!HYPERDX_URL) return;
  HyperDX.init({
    // Host-published OTLP HTTP endpoint of the module-05 collector. The browser
    // runs on the host, so this is localhost:<OTEL_HTTP_HOST_PORT>.
    url: HYPERDX_URL,
    // The collector's shared OTLP auth token (OTLP_AUTH_TOKEN), sent as the
    // ingest credential — the same secret the backend uses.
    apiKey: (import.meta.env.VITE_HYPERDX_API_KEY as string | undefined) ?? "",
    // Service name shown in HyperDX; parallels the backend's nyc-taxi-backend.
    service: (import.meta.env.VITE_HYPERDX_SERVICE as string | undefined) ?? "nyc-taxi-frontend",
    // Propagate trace headers to same-origin /api calls so a browser action links
    // to the backend span it triggered (end-to-end traces).
    tracePropagationTargets: [new RegExp(window.location.host, "i")],
    // Capture console output and full request/response detail.
    consoleCapture: true,
    advancedNetworkCapture: true,
    // Session replay is left ON (SDK default) so UI errors come with a recording.
  });
}

// Wire HyperDX into react-error-boundary so React *render* errors (which
// window.onerror never sees) are reported. Pass the ErrorBoundary component.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function attachErrorBoundary(errorBoundary: ComponentType<any>): void {
  if (!HYPERDX_URL) return;
  HyperDX.attachToReactErrorBoundary(errorBoundary);
}
