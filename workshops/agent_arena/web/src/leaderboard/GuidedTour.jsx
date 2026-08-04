import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { Icon } from '../ui.jsx'

/* A hardcoded, narrated walkthrough of the `open-vs-proprietary1` story
   (docs/open-vs-proprietary-langfuse-story.md). It drives the real Leaderboard
   UI: selects the run, sorts/filters, expands config rows, opens a conversation,
   and spotlights the element each beat is about — the leaderboard tells WHAT,
   the per-question drill-downs + LangFuse tell WHY. `ctl` is the imperative
   handle the Leaderboard hands us. */

const sel = (s) => { try { return document.querySelector(s) } catch { return null } }

export default function GuidedTour({ ctl, initialStep = 0, onStep, onExit }) {
  const ctlRef = useRef(ctl); ctlRef.current = ctl
  const [index, setIndex] = useState(initialStep)
  const [box, setBox] = useState({ hole: null, card: { left: 0, top: 0 } })
  const [link, setLink] = useState(null)
  const [busy, setBusy] = useState(true)
  const targetRef = useRef(null)
  const cardRef = useRef(null)
  const timers = useRef([])
  const addTimer = (fn, ms) => { const id = setTimeout(fn, ms); timers.current.push(id); return id }
  const clearTimers = () => { timers.current.forEach(clearTimeout); timers.current = [] }

  // ---- resolve real config_ids from the live board (robust to prompt suffixes) ----
  const board = () => ctlRef.current.getBoard()
  const winner = () => [...board()].sort((a, b) => Number(b.accuracy) - Number(a.accuracy))[0]?.config_id
  const find = (m, p) => board().find((r) => (r.model_name || '').includes(m) && (!p || (r.prompt_name || '').includes(p)))?.config_id
  const rowOf = (cid) => (cid ? sel(`[data-tour-config="${cid}"]`) : null)
  const qRowOf = (cid, qid) => (cid ? sel(`[data-tour-dd="${cid}"] tr[data-q="${qid}"]`) : null)
  const waitBoard = () => new Promise((res) => {
    const t0 = Date.now()
    const tick = () => (board().length || Date.now() - t0 > 4000 ? res() : addTimer(tick, 80))
    tick()
  })

  // ---- the hardcoded story ----
  const STEPS = [
    {
      kicker: 'Open vs Proprietary', title: 'A 2-minute read of the arena',
      body: <>6 models × 3 prompts × 18 golden questions = <b>324 graded NL→SQL attempts</b> on live ClickHouse. The leaderboard tells us <b>what</b> won; the per-question traces tell us <b>why</b>. Let’s walk the story.</>,
      before: async () => { const c = ctlRef.current; c.selectRun('open-vs-proprietary1'); c.collapseAll(); c.setPrompt('All'); c.setSort('accuracy', -1); await waitBoard() },
    },
    {
      kicker: 'The scoreboard', title: 'Claude Sonnet wins raw accuracy',
      body: <>Sorted by accuracy, <b>Claude Sonnet 4.5</b> tops the board at <b>83%</b> (15/18). That’s the headline number — but raw accuracy isn’t the whole story.</>,
      target: () => rowOf(winner()), wait: 4000,
    },
    {
      kicker: 'The value champion', title: 'Open-weight Qwen3-235b is the value pick',
      body: <>Watch the <b>$/correct</b> column: <b>Qwen3-235b</b> is one question behind Sonnet but at <b>~6× lower cost per correct answer</b> — and faster. Open beats proprietary on value.</>,
      target: () => rowOf(find('qwen')), wait: 4000,
    },
    {
      kicker: 'Cost × accuracy', title: 'See it in one chart',
      body: <>Qwen sits in the <b>cheap-and-accurate corner</b>; <b>Opus is worse <i>and</i> pricier</b> than Sonnet; the reasoning models drift up (costly) without moving right (accurate).</>,
      target: () => sel('[data-tour="scatter"]'),
    },
    {
      kicker: 'Drill 1 · why', title: 'All six models “failed” q007',
      body: <>Expand a config and find <b>q007</b> — every model got it wrong. Six identical wrong answers is the tell: they all counted cancelled/returned orders. The <b>question never said to exclude them</b> — an ambiguous golden, not a model failure.</>,
      before: async () => { const cid = find('opus') || winner(); if (cid) ctlRef.current.openOnly(cid) },
      target: () => qRowOf(find('opus') || winner(), 'q007'), wait: 5000,
    },
    {
      kicker: 'Drill 2 · open wins (1/2)', title: 'q015 — Claude got it wrong',
      body: <>Open a <b>Claude</b> config at <b>q015</b> — it’s red. Claude read “last 14 days” as <b>exclusive of today</b> and returned 14 days; the golden expected 15. A defensible reading, but off by one boundary day. Open the trace to see the date window.</>,
      before: async () => { const cid = find('claude'); if (cid) ctlRef.current.openOnly(cid) },
      target: () => qRowOf(find('claude'), 'q015'), wait: 5000,
    },
    {
      kicker: 'Drill 2 · open wins (2/2)', title: 'q015 — open models nailed it',
      body: <>Now <b>Qwen3-235b</b> at the same question — green. Qwen and DeepSeek matched the golden’s convention (<b>inclusive</b> of today). Here the open camp <b>beat both Claude models outright</b> — another spec ambiguity, only visible by diffing the traces.</>,
      before: async () => { const cid = find('qwen'); if (cid) ctlRef.current.openOnly(cid) },
      target: () => qRowOf(find('qwen'), 'q015'), wait: 5000,
    },
    {
      kicker: 'Drill 3 · operational', title: 'Reasoning models: slow & fragile',
      body: <>Sort by latency: <b>DeepSeek-R1</b> and <b>Kimi</b> burn 6–8s — ~20× the tokens of Qwen for <b>no accuracy gain</b>, and they’re format-fragile (answers get policy-rejected). A poor trade for structured NL→SQL.</>,
      before: async () => { ctlRef.current.collapseAll(); ctlRef.current.setSort('avg_latency_ms', -1) },
      target: () => sel('[data-tour-col="avg_latency_ms"]'),
    },
    {
      kicker: 'Drill 4 · trust', title: 'Don’t trust the LLM judge alone',
      body: <>The <b>LLM judge</b> loved answers that were actually wrong — it scored q007 ~0.9 because the SQL <i>looks</i> right. Judge = plausibility; <b>execution accuracy is ground truth</b>. We rank on accuracy.</>,
      before: async () => { ctlRef.current.setSort('accuracy', -1) },
      target: () => sel('[data-tour-col="avg_judge_score"]'),
    },
    {
      kicker: 'The prompt dimension', title: 'Prompt strategy barely moved the needle',
      body: <>We’re cycling the prompt filter — <b>P1 zero-shot → P2 few-shot → P3 dialect</b>. Accuracy hardly changes; the examples and cheat-sheet mostly just added latency.</>,
      before: async (_c, add) => {
        const c = ctlRef.current; c.collapseAll(); c.setPrompt('All')
        add(() => c.setPrompt('P1_zeroshot'), 700)
        add(() => c.setPrompt('P2_fewshot'), 1900)
        add(() => c.setPrompt('P3_dialect'), 3100)
        add(() => c.setPrompt('All'), 4300)
      },
      target: () => sel('[data-tour="promptfilter"]'), wait: 1500,
    },
    {
      kicker: 'The film room', title: 'Replay the whole conversation',
      body: <>Every run is a <b>LangFuse Session</b>. “View conversation” replays a model’s entire pass — watch a model reason through a tricky join, or bury the SQL in verbose thinking. It’s the difference between a number and a behavior.</>,
      before: async () => { const cid = find('deepseek') || find('kimi') || winner(); if (cid) { ctlRef.current.openOnly(cid); setTimeout(() => ctlRef.current.openConv(cid), 500) } },
      target: () => sel(`[data-tour-conv="${find('deepseek') || find('kimi') || winner()}"]`), wait: 5000,
      link: () => { const cid = find('deepseek') || find('kimi') || winner(); const u = ctlRef.current.sessionUrl?.(cid); return u ? { href: u, label: 'Open session in LangFuse' } : null },
    },
    {
      kicker: 'Takeaway', title: 'ClickHouse ran the queries · LangFuse holds the evidence',
      body: <><b>Qwen3-235b</b> is the value pick; <b>Sonnet</b> the accuracy pick; Opus and the reasoning models are poor ROI. And two “failures” (q007, q015) were <b>bugs in our questions, not the models</b> — invisible on the leaderboard, obvious in the traces.</>,
      before: async () => { const c = ctlRef.current; c.collapseAll(); c.setPrompt('All'); c.setSort('accuracy', -1) },
    },
  ]
  const N = STEPS.length
  const step = STEPS[index]

  const place = (hole) => {
    const cw = 360, ch = cardRef.current?.offsetHeight || 220, m = 16
    const vw = window.innerWidth, vh = window.innerHeight
    if (!hole) return { left: (vw - cw) / 2, top: (vh - ch) / 2 }
    let top = hole.top + hole.height + m
    if (top + ch > vh - 10) top = hole.top - ch - m
    if (top < 10) top = Math.min(vh - ch - 10, Math.max(10, hole.top))
    let left = hole.left + hole.width / 2 - cw / 2
    left = Math.max(10, Math.min(vw - cw - 10, left))
    return { left, top }
  }
  const measure = () => {
    const el = targetRef.current
    if (!el || !el.isConnected) { setBox({ hole: null, card: place(null) }); return }
    const r = el.getBoundingClientRect()
    const pad = 8
    const hole = { left: r.left - pad, top: r.top - pad, width: r.width + pad * 2, height: r.height + pad * 2 }
    setBox({ hole, card: place(hole) })
  }

  // run a step when the index changes
  useEffect(() => {
    let cancelled = false
    clearTimers(); setBusy(true); setLink(null); targetRef.current = null
    ;(async () => {
      try { await step.before?.(ctlRef.current, addTimer) } catch { /* keep going */ }
      let el = null
      if (step.target) {
        const deadline = Date.now() + (step.wait || 1200)
        el = await new Promise((res) => {
          const tick = () => {
            if (cancelled) return res(null)
            let found = null; try { found = step.target() } catch { /* */ }
            if (found) return res(found)
            if (Date.now() > deadline) return res(null)
            addTimer(tick, 70)
          }
          tick()
        })
      }
      if (cancelled) return
      targetRef.current = el
      // surface a clickable link in the card (the scrim blocks the page itself):
      // an explicit step.link, else the "trace ↗" anchor inside the spotlit row.
      let lk = null
      try { lk = step.link ? step.link(ctlRef.current) : null } catch { /* */ }
      if (!lk && el && el.querySelector) {
        const a = el.querySelector('a.trace-link[href]')
        const href = a && a.getAttribute('href')
        if (href && href !== '#') lk = { href: a.href, label: 'Open trace in LangFuse' }
      }
      setLink(lk)
      if (el) el.scrollIntoView({ block: 'center', inline: 'nearest' })
      requestAnimationFrame(() => requestAnimationFrame(() => { if (!cancelled) { measure(); setBusy(false) } }))
    })()
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [index])

  // re-place card once it has a real height
  useLayoutEffect(() => { if (!busy) measure() }, [busy]) // eslint-disable-line react-hooks/exhaustive-deps

  // keep the spotlight glued on resize; block manual scroll so it can't drift
  useEffect(() => {
    const onResize = () => measure()
    const scroller = document.querySelector('.app-body')
    const block = (e) => e.preventDefault()
    window.addEventListener('resize', onResize)
    scroller?.addEventListener('wheel', block, { passive: false })
    scroller?.addEventListener('touchmove', block, { passive: false })
    return () => {
      window.removeEventListener('resize', onResize)
      scroller?.removeEventListener('wheel', block)
      scroller?.removeEventListener('touchmove', block)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  // keyboard nav — re-bound per step so it isn't a stale closure
  useEffect(() => {
    const onKey = (e) => {
      if (e.key === 'Escape') exit()
      else if (e.key === 'ArrowRight' || e.key === 'Enter') next()
      else if (e.key === 'ArrowLeft') back()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [index])

  // remember the step in the parent so closing → reopening resumes here
  useEffect(() => { onStep?.(index) }, [index]) // eslint-disable-line react-hooks/exhaustive-deps

  const next = () => (index >= N - 1 ? finish() : setIndex((i) => i + 1))
  const back = () => setIndex((i) => Math.max(0, i - 1))
  // pause: hide the overlay but KEEP the step + the UI state, so you can talk to
  // the audience over the live UI and resume exactly where you left off.
  const exit = () => { clearTimers(); onExit(false) }
  // finish: tour is done — let the parent reset the resume point.
  const finish = () => { clearTimers(); onExit(true) }

  return (
    <>
      <div className="tour-scrim" onClick={(e) => e.stopPropagation()} />
      {box.hole && <div className="tour-hole" style={{ left: box.hole.left, top: box.hole.top, width: box.hole.width, height: box.hole.height }} />}
      <div className="tour-card" ref={cardRef} style={{ left: box.card.left, top: box.card.top }}>
        <button className="tour-x" onClick={exit} aria-label="Pause walkthrough" title="Pause — keeps your place. Reopen to resume this step."><Icon name="x" size={15} /></button>
        <div className="tour-kicker">{step.kicker}</div>
        <h3 className="tour-title">{step.title}</h3>
        <div className="tour-body">{step.body}</div>
        {link && (
          <a className="tour-link" href={link.href} target="_blank" rel="noreferrer">
            <Icon name="ext" size={14} color="var(--accent)" /> {link.label}
          </a>
        )}
        <div className="tour-foot">
          <div className="tour-dots">{STEPS.map((_, i) => <span key={i} className={i === index ? 'on' : i < index ? 'done' : ''} />)}</div>
          <div className="tour-nav">
            <span className="tour-count mono">{index + 1}/{N}</span>
            <button className="btn ghost sm" onClick={back} disabled={index === 0}>Back</button>
            <button className="btn primary sm" onClick={next}>{index >= N - 1 ? 'Finish' : 'Next'} <Icon name="chev" size={13} color="var(--accent-ink)" /></button>
          </div>
        </div>
      </div>
    </>
  )
}
