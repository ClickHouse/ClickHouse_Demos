import { useEffect, useRef, useState } from "react";

type Props = {
  /** Inlined, runnable SQL from the response `meta`. When empty the affordance is hidden. */
  sql?: string | null;
  /** Which edge the dropdown panel aligns to. "end" (default) opens leftward from a
   *  right-aligned trigger; "start" opens rightward. */
  align?: "start" | "end";
};

// A small "</>" trigger that reveals the SQL behind a panel in a click-open popover
// with a Copy button. Rendered in-flow (typically in a right-aligned strip at the top
// of a panel) so it never overlaps the panel's own controls; the popover itself is
// absolutely positioned relative to the trigger.
export function SqlPopover({ sql, align = "end" }: Props) {
  const [open, setOpen] = useState(false);
  const [copied, setCopied] = useState(false);
  const rootRef = useRef<HTMLSpanElement | null>(null);

  // Close on outside-click / Escape while open.
  useEffect(() => {
    if (!open) return;
    const onPointer = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", onPointer);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onPointer);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  if (!sql) return null;

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(sql);
    } catch {
      // clipboard API unavailable (rare on localhost/https) — fall back to execCommand.
      const ta = document.createElement("textarea");
      ta.value = sql;
      ta.style.position = "fixed";
      ta.style.opacity = "0";
      document.body.appendChild(ta);
      ta.select();
      try {
        document.execCommand("copy");
      } catch {
        /* give up silently; the SQL is still visible to copy manually */
      }
      document.body.removeChild(ta);
    }
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1500);
  };

  return (
    <span ref={rootRef} style={{ position: "relative", display: "inline-block", lineHeight: 0 }}>
      <button
        type="button"
        className="btn btn-sm btn-outline-secondary"
        title="Show the SQL that powered this panel"
        aria-label="Show SQL"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
        style={{ padding: "1px 7px", fontFamily: "monospace", fontSize: 12, lineHeight: 1.4, opacity: 0.85 }}
      >
        &lt;/&gt;
      </button>

      {open ? (
        <div
          className="shadow"
          role="dialog"
          aria-label="Executed SQL"
          style={{
            position: "absolute",
            top: "calc(100% + 6px)",
            width: 460,
            maxWidth: "78vw",
            background: "#131312",
            border: "1px solid #2b2b28",
            borderRadius: 8,
            padding: 10,
            zIndex: 30,
            ...(align === "end" ? { right: 0 } : { left: 0 })
          }}
        >
          <div className="d-flex justify-content-between align-items-center mb-2">
            <span style={{ color: "#9a9ea7", fontSize: 12, fontWeight: 600 }}>Executed SQL</span>
            <button type="button" className="btn btn-sm btn-primary py-0 px-2" onClick={copy}>
              {copied ? "Copied ✓" : "Copy"}
            </button>
          </div>
          <pre
            style={{
              margin: 0,
              maxHeight: 340,
              overflow: "auto",
              whiteSpace: "pre",
              fontSize: 12,
              lineHeight: 1.5,
              color: "#e6e7e9",
              background: "#0b0b0a",
              padding: 10,
              borderRadius: 6
            }}
          >
            {sql}
          </pre>
        </div>
      ) : null}
    </span>
  );
}
