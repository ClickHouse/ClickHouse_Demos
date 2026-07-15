# Mockup brief: ClickHouse-branded docs theme

Produce THREE self-contained static HTML mockups of a ClickHouse BUILD workshop
docs page. Each is a SINGLE .html file with all CSS inline in a `<style>` block.
No frameworks, no external JS. The ONLY allowed external resource is Google Fonts
`<link>` tags. No emojis anywhere in any file.

## Verified brand facts (extracted from clickhouse.com and clickhouse.com/docs CSS)

Palette (use these exact hex values, do not invent others):
- Signature yellow accent: #FAFF69  (bright chartreuse-yellow; the ClickHouse accent)
- Secondary/deep yellow: #FFC600     (use sparingly, e.g. hover)
- Near-black backgrounds: #161517, #1B1B1D, #1F1F1C, #151515
- Dark panel/surface: #282828, #302E32, #363636
- Dark border: #414141
- Paper/light backgrounds: #F3F2EF, #F5F5F5, #F6F7FA
- Light borders: #E6E7E9, #E0E3E8, #DFDFDF
- Muted gray text: #A0A0A0, #B3B6BD, #6B7280
- White: #FFFFFF, near-white text on dark: #F5F5F5

Typography:
- Body / UI: Inter (Google Fonts). Headings also Inter, tighter tracking.
- Monospace / code: Inconsolata (Google Fonts).
- clickhouse.com's real display face is "Söhne" (proprietary); Inter is the honest free stand-in.

Shape:
- Primary border-radius 0.25rem (4px). Pill-shaped buttons/badges allowed (large radius).
- clickhouse.com marketing is dark by default; clickhouse.com/docs is also dark (#1B1B1D bg, #FAFF69 links).

## Every mockup must contain
- Top header bar: workshop title "ClickHouse BUILD Workshop" + a Learner / Instructor toggle (two-segment control) + a search box affordance (Search + Cmd K hint).
- Left sidebar with the 10 modules (see list below), one marked active.
- Main content pane rendering the "00 Setup" module: h1, intro paragraph, a "Why"/"Goal"/"Steps" section, at least one bash code block with a visible Copy affordance, one callout/admonition box, and a "You should now have" verify checklist (checkbox list).
- Right-hand "On this page" table of contents.
- A modules table (module number / title / what you build) somewhere in the content.

## Module list (sidebar)
00 Setup (active), 01 Base app, 02 ClickHouse Cloud, 03 Real-time CDC,
04 ClickHouse Agents, 05 ClickStack, 06 AI SRE, 07 Chat and Langfuse,
08 Break and fix, 09 Wrap-up.

## Content excerpt (use verbatim-ish for realism)
Title: 00 Setup
Tagline: Go from zero accounts to a running prototype - the accounts, keys, tools,
agent skills, and app repo you need for the rest of the workshop, each one verified
before you move on.

Why: Over the next few hours you build a real-time, observable, AI-assisted analytics
application on ClickHouse Cloud - and your own coding agent does most of the typing.

Goal: By the end of this module you have a ClickHouse Cloud service with its host and
password saved, an organization API key, clickhousectl installed, your coding agent
loaded with the ClickHouse agent skills and connected to the remote ClickHouse MCP, a
Langfuse Cloud project and an OpenAI key saved, and the app running locally at
http://localhost:8080.

Bash block (Step 2 - Clone the repo):
    git clone -b build-workshop-v1 https://github.com/ClickHouse/ClickHouse_Demos.git
    cd ClickHouse_Demos/workshops/build_workshop/app
    cp .env.workshop.example .env.workshop

Callout (tip): "Do the account signups before the day if you can. Three of the steps
create external accounts - ClickHouse Cloud, Langfuse Cloud, and OpenAI. Each takes 5 to
10 minutes, mostly waiting on verification."

Verify checklist ("You should now have"):
- Docker running with 6 GB+, git, Node 20+, python3, and your coding agent signed in
- The repo cloned and a fresh .env.workshop
- A running Cloud service with CLICKHOUSE_HOST and CLICKHOUSE_PASSWORD filled in
- clickhousectl --version printing a version number

On this page (TOC): Starting point, Why, Goal, Steps, Step 1 Check your machine,
Step 2 Clone the repo, Step 3 ClickHouse Cloud account.

## The three files (distinct directions)
1. variation-1-dark.html   - clickhouse.com-faithful DARK. Near-black background (#161517),
   #FAFF69 as the accent (links, active nav item, code-block left border, checklist ticks),
   high-contrast near-white prose (#F5F5F5). Muted gray secondary text.
2. variation-2-light.html  - LIGHT docs style. White/paper background (#FFFFFF / #F6F7FA),
   near-black text (#1F1F1C), yellow reserved for PRIMARY actions and highlights only
   (active nav pill, copy button, checklist ticks). Light borders (#E6E7E9).
3. variation-3-hybrid.html - HYBRID "console" feel. Light content pane (#FFFFFF) with DARK
   chrome: dark header (#161517) and dark left sidebar (#1B1B1D) both with #FAFF69 accents.

Keep each file honest and self-contained. Semantic HTML, accessible contrast, no lorem
ipsum - use the real content above.
