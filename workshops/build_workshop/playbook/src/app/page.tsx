import type { Metadata } from 'next';
import Link from 'next/link';
import './hub.css';

// Workshops hub — the site root (/). Workshop-agnostic portal that lists the
// workshops and links to each. Add a workshop = add a card here + its module/route.
// The build workshop is a Next route (/build-workshop); the RTA mini guide is
// static under public/rta-mini/.

export const metadata: Metadata = {
  title: 'ClickHouse Workshops',
  description: 'Hands-on ClickHouse workshops — pick one and start building.',
};

// next/link handles base-path for app routes; for the static RTA guide we build
// the href with the base path explicitly.
const basePath = process.env.NEXT_PUBLIC_BASE_PATH ?? '';

export default function WorkshopsHub() {
  return (
    <div className="hub">
      <header className="top">
        <div className="wrap">
          <div className="brand">
            <svg className="brand-logo" viewBox="0 0 54 54" fill="none" role="img" aria-label="ClickHouse">
              <rect width="5.9998" height="53.9982" rx="1.45943" fill="currentColor" />
              <rect x="12" width="5.9998" height="53.9982" rx="1.45943" fill="currentColor" />
              <rect x="24.001" width="5.9998" height="53.9982" rx="1.45943" fill="currentColor" />
              <rect x="35.998" width="5.9998" height="53.9982" rx="1.45943" fill="currentColor" />
              <rect x="48.001" y="21.0005" width="5.9998" height="11.9996" rx="1.45943" fill="currentColor" />
            </svg>
            <span className="brand-name">ClickHouse</span>
            <span className="brand-sep" aria-hidden="true">/</span>
            <span className="brand-sub">Workshops</span>
          </div>
          <h1>Pick a workshop.<br />Start <span className="ai">building.</span></h1>
          <p className="lede">Hands-on ClickHouse labs — from a 90-minute analytics sprint to a full afternoon building a real-time, AI-assisted app. Each runs on your own free ClickHouse Cloud trial.</p>
        </div>
      </header>

      <section className="grid-section">
        <div className="wrap">
          <p className="eyebrow">Available workshops</p>
          <div className="grid">
            <Link className="card" href="/build-workshop">
              <div className="card-meta"><span>3 hours</span><span className="dot" aria-hidden="true"></span><span>Intermediate</span></div>
              <div className="card-title">Build AI with AI</div>
              <div className="card-tagline">Build it, end to end.</div>
              <p className="card-blurb">In one three-hour sitting, take a real analytics app live on ClickHouse Cloud with your own AI coding agent — real-time CDC, conversational BI, observability, an AI SRE, and traced AI chat.</p>
              <div className="tags"><span className="tag">CDC</span><span className="tag">Agents</span><span className="tag">ClickStack</span><span className="tag">Langfuse</span></div>
              <span className="card-cta">Open workshop <span className="arrow">→</span></span>
            </Link>

            <a className="card" href={`${basePath}/rta-mini/index.html`}>
              <div className="card-meta"><span>90 minutes</span><span className="dot" aria-hidden="true"></span><span>Beginner</span></div>
              <div className="card-title">Real-Time Market Analytics</div>
              <div className="card-tagline">26.5M forex ticks, answered in milliseconds.</div>
              <p className="card-blurb">A 90-minute hands-on lab: load ~26.5M forex ticks from object storage, run real market-analytics queries, and try the built-in AI Assistant and Agents. Optional live dashboard on your own data.</p>
              <div className="tags"><span className="tag">ClickPipes</span><span className="tag">S3</span><span className="tag">AI Assistant</span><span className="tag">dashboard</span></div>
              <span className="card-cta">Open workshop <span className="arrow">→</span></span>
            </a>
          </div>
        </div>
      </section>

      <footer>
        <div className="wrap">
          <div>ClickHouse · Workshops</div>
          <div className="links">
            <a href="https://clickhouse.com/cloud">ClickHouse Cloud</a>
            <a href="https://clickhouse.com/docs">Docs</a>
          </div>
        </div>
      </footer>
    </div>
  );
}
