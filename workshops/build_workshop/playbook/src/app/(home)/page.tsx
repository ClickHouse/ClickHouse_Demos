import Link from 'next/link';

export default function HomePage() {
  return (
    <main className="flex flex-1 flex-col items-center justify-center px-4 py-20 text-center">
      <p className="mb-4 text-sm font-medium uppercase tracking-widest text-fd-muted-foreground">
        ClickHouse BUILD Workshop
      </p>
      <h1 className="mb-6 max-w-3xl text-4xl font-bold tracking-tight sm:text-5xl">
        Build AI with AI
      </h1>
      <p className="mb-10 max-w-2xl text-lg text-fd-muted-foreground">
        A three-hour, hands-on session. You bring an agentic coding tool and take an
        NYC-taxi ride-hailing analytics app from a fresh clone to a live analytics
        platform on ClickHouse Cloud: real-time CDC, conversational BI, observability,
        AI-assisted SRE, and an in-app AI chat traced end to end.
      </p>
      <div className="flex flex-col gap-3 sm:flex-row">
        <Link
          href="/docs"
          className="rounded-lg bg-fd-primary px-6 py-3 font-medium text-fd-primary-foreground transition-opacity hover:opacity-90"
        >
          Read the overview
        </Link>
        <Link
          href="/docs/learner/00-setup"
          className="rounded-lg border border-fd-border px-6 py-3 font-medium transition-colors hover:bg-fd-accent"
        >
          Start the workshop
        </Link>
        <Link
          href="/docs/instructor/00-setup"
          className="rounded-lg border border-fd-border px-6 py-3 font-medium transition-colors hover:bg-fd-accent"
        >
          Instructor track
        </Link>
      </div>
    </main>
  );
}
