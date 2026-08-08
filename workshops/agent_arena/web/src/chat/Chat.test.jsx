/** @vitest-environment jsdom */
import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import Chat from './Chat.jsx'
import { getServing, postServing } from '../api.js'

vi.mock('../api.js', () => ({
  getServing: vi.fn(),
  postServing: vi.fn(),
}))

const completedResponse = {
  sql: 'SELECT 1',
  columns: ['n'],
  rows: [[1]],
  cost_usd: 0.00001,
  latency_ms: 12,
  outcome: 'ok',
  error: null,
  trace_id: 'trace-1',
}

function deferred() {
  let resolve
  let reject
  const promise = new Promise((done, fail) => { resolve = done; reject = fail })
  return { promise, resolve, reject }
}

async function renderCompletedTurn() {
  render(<Chat />)
  await waitFor(() => expect(screen.getByRole('combobox').value).toBe('model__prompt'))
  fireEvent.change(screen.getByPlaceholderText('Ask a question about the data…'), {
    target: { value: 'How many?' },
  })
  fireEvent.click(screen.getByRole('button', { name: 'Ask' }))
  await screen.findByText('SELECT 1')
}

describe('chat feedback', () => {
  beforeEach(() => {
    getServing.mockResolvedValue({ config_ids: ['model__prompt'] })
    postServing.mockImplementation((path) => (
      path === '/ask' ? Promise.resolve(completedResponse) : Promise.resolve({ ok: true })
    ))
  })
  afterEach(() => {
    cleanup()
    vi.clearAllMocks()
  })

  test('disables both feedback buttons only while that turn is pending', async () => {
    const feedback = deferred()
    postServing.mockImplementation((path) => (
      path === '/ask' ? Promise.resolve(completedResponse) : feedback.promise
    ))
    await renderCompletedTurn()

    fireEvent.click(screen.getByRole('button', { name: 'Thumbs down' }))

    expect(postServing).toHaveBeenLastCalledWith('/feedback', {
      trace_id: 'trace-1', value: false,
    })
    expect(screen.getByRole('button', { name: 'Thumbs up' }).disabled).toBe(true)
    expect(screen.getByRole('button', { name: 'Thumbs down' }).disabled).toBe(true)
    expect(screen.getByText('sending…')).toBeTruthy()

    await act(async () => { feedback.resolve({ ok: true }) })
    expect(screen.getByRole('button', { name: 'Thumbs up' }).disabled).toBe(false)
    expect(screen.getByRole('button', { name: 'Thumbs down' }).disabled).toBe(false)
  })

  test('selects the submitted Boolean feedback after success', async () => {
    await renderCompletedTurn()

    fireEvent.click(screen.getByRole('button', { name: 'Thumbs down' }))

    await screen.findByText('feedback sent')
    expect(screen.getByRole('button', { name: 'Thumbs down' }).dataset.on).toBe('true')
    expect(screen.getByRole('button', { name: 'Thumbs up' }).dataset.on).toBe('false')
  })

  test('keeps the prior vote and renders the error when feedback fails', async () => {
    await renderCompletedTurn()
    fireEvent.click(screen.getByRole('button', { name: 'Thumbs up' }))
    await screen.findByText('feedback sent')
    postServing.mockRejectedValueOnce(new Error('feedback unavailable'))

    fireEvent.click(screen.getByRole('button', { name: 'Thumbs down' }))

    await screen.findByText('Error: feedback unavailable')
    expect(screen.getByRole('button', { name: 'Thumbs up' }).dataset.on).toBe('true')
    expect(screen.getByRole('button', { name: 'Thumbs down' }).dataset.on).toBe('false')
    expect(screen.getByRole('button', { name: 'Thumbs up' }).disabled).toBe(false)
    expect(screen.getByRole('button', { name: 'Thumbs down' }).disabled).toBe(false)
  })
})
