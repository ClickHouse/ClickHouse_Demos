// Base URL of the FastAPI dashboard JSON API (dashboard/app.py).
// Override at build/run time with VITE_API_BASE, e.g. VITE_API_BASE=http://host:8000.
export const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000'

export async function api(path) {
  const r = await fetch(API_BASE + path)
  if (!r.ok) throw new Error(`${path} → HTTP ${r.status}`)
  return r.json()
}

// Base URL of the serving API (serving/api.py). Override with VITE_SERVING_BASE.
export const SERVING_BASE = import.meta.env.VITE_SERVING_BASE || 'http://localhost:8100'

export async function postServing(path, body) {
  const r = await fetch(SERVING_BASE + path, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!r.ok) throw new Error(`${path} → HTTP ${r.status}`)
  return r.json()
}

export async function getServing(path) {
  const r = await fetch(SERVING_BASE + path)
  if (!r.ok) throw new Error(`${path} → HTTP ${r.status}`)
  return r.json()
}
