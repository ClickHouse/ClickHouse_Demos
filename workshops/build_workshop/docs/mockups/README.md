# ClickHouse-branded docs theme mockups

Three self-contained static HTML mockups of the BUILD workshop docs site (currently
default Fumadocs styling), restyled to clickhouse.com's brand. Each file is a single
`.html` with all CSS inline; the only external resources are Google Fonts links. Open
any file directly in a browser. These are review artifacts only - no playbook source
was modified to produce them.

Generated with the OpenAI Codex CLI (`codex exec`, version 0.144.4) from `BRIEF.md`
in this directory.

## Verified brand facts

Palette and type were extracted from the live clickhouse.com and clickhouse.com/docs
CSS bundles (not guessed). Sources:
- `https://clickhouse.com` (`/_next/static/chunks/*.css`)
- `https://clickhouse.com/docs` (`/assets/css/styles.*.css`, Docusaurus `--ifm-*` vars)

Palette (exact hex values used in the mockups):

| Role | Hex |
|------|-----|
| Signature yellow accent | `#FAFF69` |
| Secondary / deep yellow (hover) | `#FFC600` |
| Near-black backgrounds | `#161517`, `#1B1B1D`, `#1F1F1C`, `#151515` |
| Dark panel / surface | `#282828`, `#302E32`, `#363636` |
| Dark border | `#414141` |
| Paper / light backgrounds | `#F3F2EF`, `#F5F5F5`, `#F6F7FA` |
| Light borders | `#E6E7E9`, `#E0E3E8`, `#DFDFDF` |
| Muted gray text | `#A0A0A0`, `#B3B6BD`, `#6B7280` |
| Near-white text on dark | `#F5F5F5` |

Typography:
- Body / UI / headings: Inter (Google Fonts). clickhouse.com's real display face is
  Soehne, which is proprietary; Inter is the honest free stand-in and is what the docs
  property itself uses for UI.
- Monospace / code: Inconsolata (Google Fonts), matching clickhouse.com's mono usage.

Shape and mode:
- Primary border-radius is `0.25rem` (4px); pill-shaped controls (toggles, badges) use a
  large radius. Both the marketing site and the docs default to a dark theme with
  `#FAFF69` as the single accent.

## The three variations

### variation-1-dark.html - clickhouse.com-faithful dark
Near-black `#161517` canvas with near-white `#F5F5F5` prose and `#FAFF69` as the only
accent: active nav item, links, the code block's left border, and the verify checkboxes
(`accent-color:#FAFF69`). This is the most literal translation of clickhouse.com's own
look. Rationale: for a workshop that lives next to the ClickHouse brand, matching the
marketing site's dark, high-contrast, single-accent language makes the docs feel like a
first-party property and keeps the yellow meaningful by using it sparingly.

### variation-2-light.html - light docs style
White / paper (`#FFFFFF`, `#F6F7FA`) background with near-black `#1F1F1C` text and light
`#E6E7E9` borders, closer to a conventional docs reading surface. Yellow is reserved for
primary moments only: the active nav pill, the copy button, and the checklist ticks.
Rationale: long-form setup instructions are read for extended stretches, and a light
surface is the lowest-fatigue default for dense prose and code. Restricting yellow to
actions keeps hierarchy clear and avoids a "highlighter" feel.

### variation-3-hybrid.html - console hybrid
Light content pane (`#FFFFFF`) framed by dark chrome: a dark header (`#161517`) and dark
left sidebar (`#1B1B1D`), both carrying `#FAFF69` accents. Rationale: this is the
"console" feel - the dark chrome echoes the ClickHouse Cloud console the workshop drives,
while the light content pane keeps the reading experience easy. It signals "you are in a
ClickHouse tool" without paying the readability cost of a fully dark long-form page.

## Mapping onto Fumadocs once a direction is picked

Fumadocs themes through CSS custom properties (the `--color-fd-*` tokens) plus Tailwind.
To adopt a variation in the playbook:

1. In `app/global.css` (or the Fumadocs theme layer), override the core tokens with the
   hex values above - primarily `--color-fd-background`, `--color-fd-foreground`,
   `--color-fd-muted`, `--color-fd-border`, `--color-fd-card`, and the primary/accent
   tokens `--color-fd-primary` / `--color-fd-accent` set to `#FAFF69`. Define both the
   `:root` (light) and `.dark` blocks; variation 1 ships dark-first, variation 2
   light-first, and variation 3 splits chrome vs. content (dark tokens scoped to the
   header/sidebar, light tokens for the article).
2. Wire the fonts in `app/layout.tsx` via `next/font/google` for Inter and Inconsolata,
   mapping them to `--font-sans` and `--font-mono` in the Tailwind config.
3. Set the accent border on code blocks and callouts and the checklist `accent-color`
   through the same tokens so the yellow stays consistent everywhere.

No hex value in these mockups is invented; each traces back to the extracted brand
palette above. There are no emojis in any file.
