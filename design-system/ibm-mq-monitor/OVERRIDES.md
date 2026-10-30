# IBM MQ Monitor — project overrides to MASTER.md

These rules override MASTER.md where they conflict. Reason for each is given.

## Palette (replaces the green-tinted MASTER palette)
Surfaces must be neutral so the three status colors carry meaning. MASTER's green background
would make every page look "healthy".

Light (`:root`):
- `--color-background: #F8FAFC`, `--color-foreground: #0F172A`
- `--color-card: #FFFFFF`, `--color-card-foreground: #0F172A`
- `--color-muted: #F1F5F9`, `--color-muted-foreground: #475569`
- `--color-border: #E2E8F0`, `--color-primary: #334155`, `--color-on-primary: #FFFFFF`, `--color-ring: #334155`

Dark (`@media (prefers-color-scheme: dark)` guarded by `:root:not([data-theme="light"])`, and `:root[data-theme="dark"]`):
- `--color-background: #020617`, `--color-foreground: #F8FAFC`
- `--color-card: #0E1223`, `--color-card-foreground: #F8FAFC`
- `--color-muted: #1A1E2F`, `--color-muted-foreground: #94A3B8`
- `--color-border: #334155`, `--color-primary: #CBD5E1`, `--color-on-primary: #0F172A`, `--color-ring: #94A3B8`

Status tokens (both modes, text contrast >= 4.5:1 on their surface):
- healthy: light text `#15803D` on `#DCFCE7`; dark text `#86EFAC` on `#14532D`
- unhealthy: light text `#B91C1C` on `#FEE2E2`; dark text `#FCA5A5` on `#7F1D1D`
- unknown / waiting: light text `#475569` on `#F1F5F9`; dark text `#CBD5E1` on `#1E293B`
- destructive button: `#DC2626` with `#FFFFFF`

Never convey health by color alone: every health cell shows a dot **and** text (`healthy`,
`unhealthy`, `n/a`), with the reason as visible text on the dashboard and as `title` in tables.

## Typography (replaces Fira via Google Fonts)
No CDN is allowed (air-gapped deployments). Use system stacks:
- body: `system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif`
- numbers and identifiers (queue names, depths, hosts): `ui-monospace, SFMono-Regular, Menlo, Consolas, monospace`
- base 16px, line-height 1.5; table cells 14px; `font-variant-numeric: tabular-nums` on numeric cells; numbers right-aligned.

## Layout and density (MASTER density 8 stands)
- Spacing scale 4/8/12/16/24/32px. Max content width 1440px, 16px side gutter, no horizontal page scroll; tables get an `overflow-x: auto` wrapper.
- Dashboard: queue manager card on top (name, status, platform, command level, sample time, connection verdict), then the queue table, then the channel table. Readable at 1280px with 50 queues.
- Form: labels above inputs, required marker, errors next to the field, `<details>` for Advanced, TLS fieldset only when mode != NONE.
- Buttons and links: min 44px tall hit area, visible 2px focus outline with 2px offset, hover transition 150-200ms, `cursor: pointer`.
- Motion: none beyond hover/focus transitions. Respect `prefers-reduced-motion`.
- Icons: inline SVG only (no emoji). A single 8px status dot is enough for health.
- Stale data: the qmgr card shows the sample time; when the SSE connection drops, HTMX adds `.htmx-request`/error classes — style a visible "disconnected" state on the dashboard container.
