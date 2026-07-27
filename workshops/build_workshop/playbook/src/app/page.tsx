import type { Metadata } from 'next';
import Link from 'next/link';
import './landing.css';
import { PlatformSelector } from '@/components/platform';

// Workshop landing page (the site root, /). Ported from the standalone one-pager;
// styles live in landing.css, scoped under `.op`. CTAs use next/link so they are
// base-path-aware (resolve under both the local root and the /workshop prod base path).

export const metadata: Metadata = {
  title: 'Build it, end to end — ClickHouse BUILD Workshop',
  description:
    'A three-hour, hands-on workshop: your own AI coding agent takes a real analytics app end to end on ClickHouse Cloud — real-time CDC, conversational BI, observability, an AI SRE, and traced AI chat.',
};

export default function LandingPage() {
  return (
    <div className="op">
      {/* ===================== HERO ===================== */}
      <header className="hero">
        <div className="wrap">
          <div className="hero-grid">
            <div>
              <p className="eyebrow"><span className="tick">ClickHouse</span>{' '} BUILD Workshop</p>
              <h1>Build it,<br /><span className="ai">end to end.</span></h1>
              <p className="hero-sub">
                In one three-hour sitting, you take a real analytics app live on ClickHouse Cloud —
                hands-on, module by module, with your AI coding agent working alongside you.
              </p>
              <div className="chips">
                <span className="chip"><b>3{' '}hours</b> · start to finish</span>
                <span className="chip"><b>100%</b> hands-on</span>
                <span className="chip"><b>$0</b> on trial credits</span>
                <span className="chip">bring your <b>own agent</b></span>
              </div>
              <div className="landing-platform-choice">
                <PlatformSelector />
                <p>Choose once. The workshop keeps the right setup visible on every page.</p>
              </div>
              <div className="cta">
                <div className="cta-row">
                  <Link className="btn btn-primary" href="/docs/learner/00-setup">Start the workshop <span className="arrow">→</span></Link>
                  <Link className="btn btn-ghost" href="/docs">Read the overview</Link>
                </div>
                <Link className="cta-tertiary" href="/docs/instructor/00-setup">Instructor track <span className="arrow">→</span></Link>
              </div>
            </div>

            <div className="wf-diagram" role="img" aria-label="Workshop data flow: managed Postgres streams through a ClickPipes CDC pipe into ClickHouse Cloud; the app and ClickHouse Agents read from ClickHouse, while the app sends telemetry to ClickStack and its AI chat sends traces to Langfuse.">
              <div className="wf-cap"><span className="wf-live" aria-hidden="true"></span> live data flow</div>
              <div className="wf-stage">
                <svg className="wf-edges" viewBox="0 0 440 400" preserveAspectRatio="xMidYMid meet" aria-hidden="true">
                  <defs>
                    <marker id="wf-arrow" viewBox="0 0 10 10" refX="8.5" refY="5" markerWidth="7.5" markerHeight="7.5" orient="auto-start-reverse">
                      <path d="M1,1 L9,5 L1,9 z" fill="#E7EC4A" />
                    </marker>
                  </defs>
                  {/* directed edges — each ends just above its destination so the arrowhead shows */}
                  <path className="wf-edge" markerEnd="url(#wf-arrow)" d="M220,40 L220,121" />
                  <path className="wf-edge" markerEnd="url(#wf-arrow)" d="M220,145 C186,180 160,205 146,228" />
                  <path className="wf-edge" markerEnd="url(#wf-arrow)" d="M220,145 C254,180 280,205 294,228" />
                  <path className="wf-edge" markerEnd="url(#wf-arrow)" d="M140,250 C114,285 92,315 82,335" />
                  <path className="wf-edge" markerEnd="url(#wf-arrow)" d="M140,250 C160,288 186,315 198,335" />
                  {/* animated flow overlay: dashes travel source → destination */}
                  <path className="wf-flow" d="M220,40 L220,121" />
                  <path className="wf-flow" d="M220,145 C186,180 160,205 146,228" style={{ animationDelay: '.30s' }} />
                  <path className="wf-flow" d="M220,145 C254,180 280,205 294,228" style={{ animationDelay: '.15s' }} />
                  <path className="wf-flow" d="M140,250 C114,285 92,315 82,335" style={{ animationDelay: '.45s' }} />
                  <path className="wf-flow" d="M140,250 C160,288 186,315 198,335" style={{ animationDelay: '.60s' }} />
                  {/* edge labels */}
                  <text className="wf-label" x="230" y="82">ClickPipes</text>
                  <text className="wf-label" x="230" y="96">CDC</text>
                  <text className="wf-label" x="16" y="300">telemetry</text>
                  <text className="wf-label" x="206" y="300">traces</text>
                  {/* traveling data packets */}
                  <circle className="wf-packet wf-p1" r="3.4" />
                  <circle className="wf-packet wf-p2" r="3.4" />
                  <circle className="wf-packet wf-p3" r="3.4" />
                  <circle className="wf-packet wf-p4" r="3.4" />
                  <circle className="wf-packet wf-p5" r="3.4" />
                </svg>

                <div className="wf-node wf-src" style={{ left: '50%', top: '10%' }}>
                  <div className="wf-name">Managed Postgres</div><div className="wf-role">source</div>
                </div>
                <div className="wf-node wf-hub" style={{ left: '50%', top: '36.25%' }}>
                  <div className="wf-lockup">
                    <svg className="wf-logo" viewBox="0 0 54 54" fill="none" aria-hidden="true">
                      <rect width="5.9998" height="53.9982" rx="1.45943" fill="currentColor" />
                      <rect x="12" width="5.9998" height="53.9982" rx="1.45943" fill="currentColor" />
                      <rect x="24.001" width="5.9998" height="53.9982" rx="1.45943" fill="currentColor" />
                      <rect x="35.998" width="5.9998" height="53.9982" rx="1.45943" fill="currentColor" />
                      <rect x="48.001" y="21.0005" width="5.9998" height="11.9996" rx="1.45943" fill="currentColor" />
                    </svg>
                    <span className="wf-name">ClickHouse Cloud</span>
                  </div>
                  <div className="wf-role">store · analyze</div>
                </div>
                <div className="wf-node" style={{ left: '31.8%', top: '62.5%' }}>
                  <div className="wf-name">App</div><div className="wf-role">dashboards · chat</div>
                </div>
                <div className="wf-node" style={{ left: '68.2%', top: '62.5%' }}>
                  <div className="wf-name">Agents</div><div className="wf-role">BI</div>
                </div>
                <div className="wf-node" style={{ left: '18.2%', top: '89.5%' }}>
                  <div className="wf-name">ClickStack</div><div className="wf-role">observe</div>
                </div>
                <div className="wf-node" style={{ left: '45.5%', top: '89.5%' }}>
                  <div className="wf-lockup">
                    <svg className="wf-lf-logo" viewBox="0 0 512 512" fill="none" aria-hidden="true">
                      <path d="M254.75 302.25L285.25 326.75C285.25 326.75 308.587 309.418 325.75 306.875C343.75 304.208 362.954 314.244 380.75 326.208C407.629 344.279 430.25 367.208 430.25 367.208L456.75 341.208C456.75 341.208 383.686 262.047 325.75 269.208C287.75 273.905 254.75 302.25 254.75 302.25Z" fill="#FF5D5F" />
                      <path d="M80.25 151.286L55.25 178.786C55.25 178.786 124.902 243.786 179.75 243.786C204.75 243.786 239.419 224.201 269.25 198.757C286.25 184.257 305.25 167.786 324.25 167.786C337.021 167.786 353.866 174.551 369.75 192.316C369.75 192.316 380.003 186.168 386.25 181.75C391.74 177.868 399.896 171.25 399.896 171.25C377.047 146.864 343.998 129.038 324.25 130.786C292.25 130.79 269.25 150.711 240.75 173.75C212.25 196.789 200.25 206.286 179.75 206.286C145.25 206.286 80.25 151.286 80.25 151.286Z" fill="#4E9CFF" />
                      <path d="M80.25 360.75L55.25 333.25C55.25 333.25 124.902 268.25 179.75 268.25C204.75 268.25 239.419 287.835 269.25 313.279C286.25 327.779 305.25 344.25 324.25 344.25C337.083 344.25 353.799 337.207 369.75 319.25C369.75 319.25 379.339 325.161 385.25 329.25C391.328 333.455 400.25 340.407 400.25 340.407C377.39 364.987 344.1 383.007 324.25 381.25C292.25 381.246 273.25 364.289 244.75 341.25C216.25 318.211 200.25 305.75 179.75 305.75C145.25 305.75 80.25 360.75 80.25 360.75Z" fill="#4E9CFF" />
                      <path d="M406.25 213.25C399.745 217.746 389.25 224.25 389.25 224.25C389.25 224.25 395.25 237.25 395.25 254.75C395.25 272.25 389.75 287.25 389.75 287.25C389.75 287.25 399.172 293.135 405.25 297.25C411.564 301.525 421.25 308.75 421.25 308.75C421.25 308.75 432.75 284.75 432.75 254.75C432.75 224.75 421.25 202.25 421.25 202.25C421.25 202.25 412.226 209.12 406.25 213.25Z" fill="#4E9CFF" />
                      <path d="M256.25 209.25L285.25 185.25C285.25 185.25 308.587 202.04 325.75 204.583C343.75 207.25 362.954 197.214 380.75 185.25C407.629 167.179 430.25 144.25 430.25 144.25L456.75 170.25C456.75 170.25 383.686 249.411 325.75 242.25C287.75 237.553 256.25 209.25 256.25 209.25Z" fill="#FF5D5F" />
                      <path d="M186.255 130.25C223.755 130.25 255.25 162.25 255.25 162.25C255.25 162.25 246.487 169.155 240.75 173.75C234.775 178.536 225.25 186.25 225.25 186.25C225.25 186.25 208.755 168.75 186.255 168.75C177.028 168.75 165.039 174.292 152.255 185.25C142.391 193.705 132.129 204.216 125.255 217.25C119.31 228.52 116.068 241.802 115.755 255.75C115.361 273.269 121.571 291.634 131.755 306.25C138.58 316.046 146.726 323.418 155.255 329.75C166.323 337.968 177.865 343.75 186.255 343.75C195.217 343.75 203.274 340.635 209.255 337.75C218.755 332.25 226.25 325.75 226.25 325.75L255.75 350.25C255.75 350.25 243.75 362.25 227.255 371.25C216.595 376.507 202.895 381.75 186.255 381.75C169.626 381.75 150.315 372.915 132.255 359.25C120.579 350.416 109.135 339.948 100.255 327.25C85.7005 306.438 78.2004 281.118 78.2502 255.75C78.3008 230.065 86.5823 204.625 101.255 183.75C124.255 153.75 158.273 130.25 186.255 130.25Z" fill="#FF5D5F" />
                    </svg>
                    <span className="wf-name">Langfuse</span>
                  </div>
                  <div className="wf-role">LLM traces</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </header>
      <div className="checker" aria-hidden="true"></div>

      {/* ===================== HOOK ===================== */}
      <section className="section hook">
        <div className="wrap">
          <p className="lead">Not a lecture. You leave with a <span className="mark">running prototype.</span></p>
          <div className="hook-cols">
            <div>
              <p>
                Most workshops hand you slides. This one hands you a real, working NYC-taxi
                ride-hailing app — React front end, FastAPI back end, a Postgres source — and, module by
                module, you move its data and its intelligence onto ClickHouse Cloud.
              </p>
              <p>
                You never build from scratch and you never fall behind. Every command is copy-paste and
                <strong> your coding agent handles the typing, so you can focus on learning each piece</strong>.
                Each module states exactly what you need in place — so if you get stuck, you catch up in seconds.
              </p>
              <p>
                When the session ends, the trial keeps running, everything you built keeps running on it,
                and <strong>the repository is yours to take back to your team</strong> and demo live.
              </p>
            </div>
            <div className="fromto" role="list" aria-label="Where you start and where you finish">
              <div className="row" role="listitem">
                <span className="a">local app</span><span className="to"></span><span className="b">live on ClickHouse Cloud</span>
              </div>
              <div className="row" role="listitem">
                <span className="a">static data</span><span className="to"></span><span className="b">real-time CDC stream</span>
              </div>
              <div className="row" role="listitem">
                <span className="a">SQL by hand</span><span className="to"></span><span className="b">conversational BI</span>
              </div>
              <div className="row" role="listitem">
                <span className="a">blind incidents</span><span className="to"></span><span className="b">AI SRE + traced chat</span>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* ===================== OUTCOMES ===================== */}
      <section className="section outcomes">
        <div className="wrap">
          <div className="head">
            <p className="eyebrow">What you walk away with</p>
            <h2>Everything you&apos;ll have running by the end.</h2>
            <p>All on your own ClickHouse Cloud trial — you connect it, module by module, during the session.</p>
          </div>
          <div className="cards">
            <article className="card">
              <span className="n">01</span><div className="bar"></div>
              <h3>A live ops dashboard</h3>
              <p>Over <b>3.2M+ real NYC taxi rows</b>, seeded straight from object storage — and fast enough to feel it.</p>
            </article>
            <article className="card">
              <span className="n">02</span><div className="bar"></div>
              <h3>A real-time CDC pipeline</h3>
              <p>Your own managed <b>Postgres streaming continuously into ClickHouse</b> through a ClickPipe.</p>
            </article>
            <article className="card">
              <span className="n">03</span><div className="bar"></div>
              <h3>Conversational BI</h3>
              <p>Ask your data questions in plain language with <b>ClickHouse Agents</b> — governed, no SQL required.</p>
            </article>
            <article className="card">
              <span className="n">04</span><div className="bar"></div>
              <h3>Full observability + an AI SRE</h3>
              <p>App traces and logs flow into <b>ClickStack / HyperDX</b>; your agent builds a dashboard and alert over them, then diagnoses a live incident.</p>
            </article>
            <article className="card">
              <span className="n">05</span><div className="bar"></div>
              <h3>Traced AI chat</h3>
              <p>An in-app AI chat with <b>every turn, generation, and cost traced end to end</b> in Langfuse.</p>
            </article>
            <article className="card" style={{ background: 'var(--ink)', color: '#fff', borderColor: 'var(--ink)' }}>
              <span className="n" style={{ color: 'var(--yellow)' }}>+</span><div className="bar"></div>
              <h3 style={{ color: '#fff' }}>The whole repo</h3>
              <p style={{ color: 'rgba(255,255,255,.7)' }}>Yours to keep, extend, and point at <b style={{ color: '#fff' }}>your own data</b> after the room clears.</p>
            </article>
          </div>
        </div>
      </section>

      {/* ===================== PLATFORM ===================== */}
      <section className="section platform on-dark">
        <div className="wrap">
          <div className="head">
            <p className="eyebrow">One platform, end to end</p>
            <h2>Every ClickHouse product you&apos;ll use.</h2>
            <p>A guided tour across ClickHouse Cloud — you connect and configure each piece yourself, from ingestion to AI.</p>
          </div>
          <div className="products">
            <div className="prod-card"><div className="role">Platform + database</div><div className="name">ClickHouse Cloud</div><div className="desc">The managed service everything runs on — millions of rows, sub-second queries.</div></div>
            <div className="prod-card"><div className="role">Ingestion</div><div className="name">ClickPipes</div><div className="desc">Postgres CDC that streams your live rows into ClickHouse.</div></div>
            <div className="prod-card"><div className="role">Source database</div><div className="name">Managed Postgres</div><div className="desc">Postgres, managed by ClickHouse — your CDC source, created with clickhousectl.</div></div>
            <div className="prod-card"><div className="role">Conversational BI</div><div className="name">ClickHouse Agents</div><div className="desc">Ask your data questions in plain language, governed by RBAC.</div></div>
            <div className="prod-card"><div className="role">Observability</div><div className="name">ClickStack + HyperDX</div><div className="desc">Your app&apos;s traces and logs, searchable in the HyperDX UI.</div></div>
            <div className="prod-card"><div className="role">LLM observability</div><div className="name">Langfuse</div><div className="desc">Trace every AI chat turn — generations, latency, and cost — end to end.</div></div>
            <div className="prod-card"><div className="role">Agent integration</div><div className="name">ClickHouse MCP</div><div className="desc">The MCP endpoints your coding agent connects to, over OAuth, to drive it all.</div></div>
          </div>
          <p className="foot">Plus the one non-ClickHouse service the app calls: <b>OpenAI</b> (the chat model behind the in-app AI chat).</p>
        </div>
      </section>

      {/* ===================== AGENDA / ROUTE ===================== */}
      <section className="section agenda">
        <div className="wrap">
          <div className="head">
            <p className="eyebrow">The route · ~2h30 hands-on</p>
            <h2>Ten stops, in order.</h2>
            <p>A three-hour session: ~2h30 of building, plus the opening, transitions, and the live finale demos.</p>
          </div>
          <ol className="route">
            <li className="stop"><div className="stop-marker">00</div><div className="stop-body">
              <div className="stop-head"><h3>Setup</h3><span className="stop-time">25 min</span></div>
              <p>Accounts, keys, tools, agent skills, and the app running locally — all wired up and verified.</p></div></li>
            <li className="stop"><div className="stop-marker">01</div><div className="stop-body">
              <div className="stop-head"><h3>ClickHouse Cloud</h3><span className="stop-time">15 min</span></div>
              <p>Create the schema, seed 3.2M+ historical rows from object storage, and feel the query speed.</p></div></li>
            <li className="stop"><div className="stop-marker">02</div><div className="stop-body">
              <div className="stop-head"><h3>Base app</h3><span className="stop-time">5 min</span></div>
              <p>Tour the running app now that it has data: the Ops and Historical dashboards and the chat panel.</p></div></li>
            <li className="stop"><div className="stop-marker">03</div><div className="stop-body">
              <div className="stop-head"><h3>Real-time CDC</h3><span className="stop-time">20 min</span></div>
              <p>Stream live rows from your own managed Postgres into ClickHouse with a Postgres CDC ClickPipe.</p></div></li>
            <li className="stop"><div className="stop-marker">04</div><div className="stop-body">
              <div className="stop-head"><h3>ClickHouse Agents</h3><span className="stop-time">10 min</span></div>
              <p>Conversational BI: create an agent over your taxi data and explore it in natural language.</p></div></li>
            <li className="stop"><div className="stop-marker">05</div><div className="stop-body">
              <div className="stop-head"><h3>ClickStack</h3><span className="stop-time">15 min</span></div>
              <p>Enable ClickStack and send your app&apos;s traces and logs to HyperDX.</p></div></li>
            <li className="stop"><div className="stop-marker">06</div><div className="stop-body">
              <div className="stop-head"><h3>AI SRE</h3><span className="stop-time">15 min</span></div>
              <p>Connect your agent to the ClickStack MCP and have it build an SRE dashboard and an alert.</p></div></li>
            <li className="stop"><div className="stop-marker">07</div><div className="stop-body">
              <div className="stop-head"><h3>Break and fix</h3><span className="stop-time">20 min</span></div>
              <p>Inject a realistic fault, diagnose it from telemetry with an AI SRE, and ship the fix.</p></div></li>
            <li className="stop"><div className="stop-marker">08</div><div className="stop-body">
              <div className="stop-head"><h3>Chat and Langfuse</h3><span className="stop-time">15 min</span></div>
              <p>Use the in-app AI chat and follow its traces, generations, and costs in Langfuse.</p></div></li>
            <li className="stop"><div className="stop-marker">09</div><div className="stop-body">
              <div className="stop-head"><h3>Wrap-up</h3><span className="stop-time">10 min</span></div>
              <p>Review what you built, take it home, and extend it to your own data.</p></div></li>
          </ol>
          <p className="note"><b>Runs with an instructor or fully self-paced.</b> The primary path is self-serve — your coding agent doubles as your instructor, and a troubleshooting reference covers every failure seen in testing.</p>
        </div>
      </section>

      {/* ===================== WHO / BRING / FORMAT ===================== */}
      <section className="section details">
        <div className="wrap">
          <div className="head">
            <p className="eyebrow">Before you join</p>
            <h2>Who it&apos;s for, and what to bring.</h2>
          </div>
          <div className="detail-grid">
            <div className="detail">
              <h3>Who it&apos;s for <span className="tag">audience</span></h3>
              <ul>
                <li>Solutions architects and pre-sales engineers at partners</li>
                <li>Data and platform engineers evaluating ClickHouse</li>
                <li>Technical teams at partners&apos; customers who want the full stack, hands-on</li>
                <li><span>Comfortable in a terminal and working alongside an AI coding agent</span></li>
              </ul>
            </div>
            <div className="detail">
              <h3>What to bring <span className="tag">prerequisites</span></h3>
              <ul>
                <li>A laptop with <b>Docker</b> running</li>
                <li>Your own <b>agentic coding tool</b> — Claude Code, Cursor, Codex CLI, or Windsurf — signed in on an active plan</li>
                <li>A <b>ClickHouse Cloud</b> account with trial credits <span>(created in prework)</span></li>
                <li>An LLM for the chat module — <b>bring your own OpenAI-compatible model</b> (Azure, OpenRouter, vLLM, or local), or ~$5 of OpenAI credit <span>that stretches across many runs</span></li>
              </ul>
            </div>
            <div className="detail">
              <h3>How it runs <span className="tag">format</span></h3>
              <ul>
                <li><b>100% hands-on</b>, module by module — you configure and connect, you don&apos;t edit app code</li>
                <li>Every command and query is <b>copy-paste</b></li>
                <li><b>Never stranded:</b> each module names a checkpoint you can start fresh from</li>
                <li><span>Your dashboard is live on real data inside the first 40 minutes</span></li>
              </ul>
            </div>
          </div>
        </div>
      </section>

      {/* ===================== CLOSING CTA ===================== */}
      <section className="close">
        <div className="checker" aria-hidden="true"></div>
        <div className="section wrap">
          <div className="inner">
            <div>
              <h2>Ready to build?</h2>
              <p>Bring your team, your laptops, and your coding agents. Leave with a real-time, observable, AI-assisted analytics app on ClickHouse Cloud.</p>
              <div className="cta" style={{ marginTop: '26px' }}>
                <div className="cta-row">
                  <Link className="btn btn-primary" href="/docs/learner/00-setup">Start the workshop <span className="arrow">→</span></Link>
                  <Link className="btn btn-ghost" href="/docs">Read the overview</Link>
                </div>
                <Link className="cta-tertiary" href="/docs/instructor/00-setup">Instructor track <span className="arrow">→</span></Link>
              </div>
            </div>
            <div className="facts">
              <div className="fact"><span className="big">$0</span><span>ClickHouse Cloud trial credits cover the whole workshop.</span></div>
              <div className="fact"><span className="big">30 days</span><span>Your trial and everything you built keep running afterward.</span></div>
              <div className="fact"><span className="big">Yours</span><span>The repository is yours to keep, extend, and demo.</span></div>
            </div>
          </div>
        </div>
      </section>

      {/* ===================== FOOTER ===================== */}
      <footer>
        <div className="wrap">
          <div className="brand">ClickHouse <b>BUILD</b> Workshop — Build AI with AI</div>
          <div className="links">
            <Link href="/docs">Playbook</Link>
            <a href="https://clickhouse.com/cloud">ClickHouse Cloud</a>
            <a href="https://clickhouse.com/docs">Docs</a>
          </div>
        </div>
      </footer>
    </div>
  );
}
