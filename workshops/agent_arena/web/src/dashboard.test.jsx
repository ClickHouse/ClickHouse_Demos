/** @vitest-environment jsdom */
import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import App from './App.jsx'
import Leaderboard from './leaderboard/Leaderboard.jsx'
import { api } from './api.js'

vi.mock('./api.js', () => ({
  API_BASE: 'http://api.test',
  SERVING_BASE: 'http://serving.test',
  api: vi.fn(),
}))

function deferred() {
  let resolve
  const promise = new Promise((done) => { resolve = done })
  return { promise, resolve }
}

function resolvedApi(path) {
  if (path === '/api/runs') return Promise.resolve([])
  if (path === '/api/meta') return Promise.resolve({})
  if (path === '/api/grid-options') return Promise.resolve({ prompts: [] })
  return Promise.resolve([])
}

describe('workshop dashboard surface', () => {
  beforeEach(() => api.mockImplementation(resolvedApi))
  afterEach(() => cleanup())

  test('only exposes the leaderboard and chat workshop tabs', async () => {
    render(<App />)

    expect(screen.getByRole('button', { name: 'Leaderboard' })).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Chat' })).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'Countdown' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Architecture' })).toBeNull()

    fireEvent.click(screen.getByRole('button', { name: 'Leaderboard' }))
    expect(screen.queryByText(/Build the arena/i)).toBeNull()
    expect(screen.queryByText(/Guided walkthrough/i)).toBeNull()
  })

  test('shows loading states for the run list and selected run data', async () => {
    const runs = deferred()
    const board = deferred()
    const tiers = deferred()
    const outcomes = deferred()
    api.mockImplementation((path) => {
      if (typeof path !== 'string') return Promise.resolve([])
      if (path === '/api/runs') return runs.promise
      if (path === '/api/meta') return Promise.resolve({})
      if (path === '/api/grid-options') return Promise.resolve({ prompts: [] })
      if (path.startsWith('/api/leaderboard')) return board.promise
      if (path.startsWith('/api/tiers')) return tiers.promise
      if (path.startsWith('/api/outcomes')) return outcomes.promise
      return Promise.resolve([])
    })

    render(<Leaderboard />)
    const select = screen.getByRole('combobox')
    expect(select.disabled).toBe(true)
    expect(screen.getByRole('option', { name: 'Loading runs…' })).toBeTruthy()

    await act(async () => { runs.resolve(['actual-model-run']) })
    expect(await screen.findByText('Loading dashboard data')).toBeTruthy()
    expect(select.disabled).toBe(true)

    await act(async () => {
      board.resolve([])
      tiers.resolve([])
      outcomes.resolve([])
    })
    await waitFor(() => expect(screen.queryByText('Loading dashboard data')).toBeNull())
    expect(select.disabled).toBe(false)
    expect(screen.getByRole('option', { name: 'actual-model-run' })).toBeTruthy()
  })
})
