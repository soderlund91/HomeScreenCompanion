---
version: 1
name: Home-Screen-Companion-design
description: "Linear's restraint applied to an Emby plugin. A quiet, dense settings surface that sits inside Emby's own theme (dark or light): surfaces and text come from Emby's theme variables, depth comes from a surface ladder plus hairline borders instead of shadows, and the Emby green (#52B54B) is the single primary accent — used on the primary action, the active tab, focus and success, never as decoration. Emby blue (#00a4dc) is a second, strictly functional accent for 'running / in progress'. Sentence-case labels, weight 500–600, small tinted status pills, 8px corners."

colors:
  # Accents (fixed — same in dark and light themes)
  primary: "#52B54B"            # Emby green
  primary-hover: "#5fc458"
  primary-pressed: "#3a8c34"
  primary-text-light: "#3a8c34" # green used as TEXT on the light theme (#52B54B is too low contrast on white)
  on-primary: "#ffffff"
  activity: "#00a4dc"           # Emby blue — running, in progress, "Run now"
  activity-hover: "#22b4e6"
  activity-text-light: "#007ba8"
  on-activity: "#ffffff"

  # Semantic
  success: "#52B54B"
  warning: "#e6c15a"
  warning-text-light: "#9a7a12"
  danger: "#e57373"
  danger-strong: "#cc3333"      # destructive buttons, failed dot
  neutral: "#9aa4b0"            # skipped / inactive

  # Surfaces — always Emby theme variables, never hard-coded
  canvas: "var(--theme-background-level1)"
  surface-1: "var(--theme-background-level2)"
  surface-2: "var(--theme-background-level3)"
  tint-1: "rgba(128,128,128,0.04)"   # header strips, subtle rows
  tint-2: "rgba(128,128,128,0.08)"   # hover
  tint-3: "rgba(128,128,128,0.15)"   # selected / pressed
  hairline: "var(--line-color)"
  hairline-strong: "rgba(128,128,128,0.3)"

  # Text
  ink: "var(--theme-text-primary)"
  ink-muted: "var(--theme-text-secondary)"
  ink-subtle: "var(--theme-text-secondary) at 70% opacity"

  # Popups / modals (set per theme in configPage.js)
  popup-bg: "var(--plugin-popup-bg)"         # #2a2a2a dark / #f2f2f2 light
  popup-bg-2: "var(--plugin-popup-bg2)"      # #333333 / #e0e0e0
  popup-ink: "var(--plugin-popup-color)"     # #e8e8e8 / #1a1a1a
  popup-ink-muted: "var(--plugin-popup-muted)"
  popup-hairline: "var(--plugin-popup-border)"
  popup-hover: "var(--plugin-popup-hover)"
  popup-badge: "var(--plugin-popup-badge)"

  # Category icon colours (type chips: neutral chip, coloured icon only). Light-theme value after the slash.
  cat-tag: "#9aa4b0 / #6b7580"
  cat-collection: "#9b7fd4 / #6f45b5"
  cat-playlist: "#3cbfa0 / #1f8c74"
  cat-toplist: "#d4b45a / #8f7224"
  cat-imported: "#e09550 / #b0661c"
  cat-movies: "#b8a28c / #806a55"
  cat-shows: "#a29dbb / #6b668a"

  # Art customiser: dark tile behind the white font samples (same in both themes)
  art-tile: "#26262b"

  # Log viewer (always dark, independent of theme)
  log-bg: "#1a1a1a"
  log-ink: "#d0d0d0"
  log-ts: "#6a6a6a"
  log-ok: "#7ec97e"
  log-warn: "#e6c15a"
  log-err: "#ef6e6e"
  log-skip: "#9aa4b0"
  log-debug: "#6f7f8f"

typography:
  # Font family is inherited from Emby — never set one. Sizes in em so Emby's scaling applies.
  page-title:   { fontSize: 1.6em,  fontWeight: 600, lineHeight: 1.2, letterSpacing: -0.3px }
  section-title:{ fontSize: 1.05em, fontWeight: 600, lineHeight: 1.3, letterSpacing: -0.1px }
  card-title:   { fontSize: 1em,    fontWeight: 600, lineHeight: 1.35 }
  body:         { fontSize: 0.95em, fontWeight: 400, lineHeight: 1.5 }
  body-sm:      { fontSize: 0.88em, fontWeight: 400, lineHeight: 1.5 }
  caption:      { fontSize: 0.8em,  fontWeight: 400, lineHeight: 1.4 }
  label:        { fontSize: 0.88em, fontWeight: 500, lineHeight: 1.3 }
  tab:          { fontSize: 0.9em,  fontWeight: 500, lineHeight: 1.2 }
  button:       { fontSize: 0.9em,  fontWeight: 500, lineHeight: 1.2 }
  eyebrow:      { fontSize: 0.75em, fontWeight: 600, lineHeight: 1.3, letterSpacing: 0.6px, textTransform: uppercase }
  mono:         { fontFamily: "ui-monospace, Consolas, Menlo, monospace", fontSize: 0.85em, lineHeight: 1.5 }

rounded:
  xs: 4px
  sm: 6px
  md: 8px
  lg: 12px
  pill: 9999px

spacing:
  xxs: 4px
  xs: 8px
  sm: 12px
  md: 16px
  lg: 24px
  xl: 32px

components:
  button-primary:      { backgroundColor: "{colors.primary}", textColor: "{colors.on-primary}", typography: "{typography.button}", rounded: "{rounded.md}", padding: "8px 16px" }
  button-primary-hover:{ backgroundColor: "{colors.primary-hover}" }
  button-activity:     { backgroundColor: "{colors.activity}", textColor: "{colors.on-activity}", typography: "{typography.button}", rounded: "{rounded.md}", padding: "8px 16px" }
  button-secondary:    { backgroundColor: "{colors.tint-1}", textColor: "{colors.ink}", border: "1px solid {colors.hairline}", typography: "{typography.button}", rounded: "{rounded.md}", padding: "8px 16px" }
  button-ghost:        { backgroundColor: transparent, textColor: "{colors.ink-muted}", typography: "{typography.button}", rounded: "{rounded.md}", padding: "6px 10px" }
  button-danger:       { backgroundColor: transparent, textColor: "{colors.danger}", border: "1px solid {colors.danger} at 40%", rounded: "{rounded.md}" }
  fab:                 { height: 44px, rounded: "{rounded.md}", padding: "0 18px", shadow: "0 4px 16px rgba(0,0,0,0.35)" }
  card:                { backgroundColor: "{colors.surface-1}", border: "1px solid {colors.hairline}", rounded: "{rounded.md}", padding: "{spacing.lg}" }
  collapsible-row:     { backgroundColor: "{colors.surface-1}", border: "1px solid {colors.hairline}", rounded: "{rounded.md}", headerPadding: "12px 16px", bodyBackground: "{colors.canvas}", bodyPadding: "{spacing.md} {spacing.lg}" }
  page-tab:            { textColor: "{colors.ink-muted}", activeTextColor: "{colors.ink}", activeIndicator: "2px {colors.primary} underline", typography: "{typography.tab}", padding: "10px 16px" }
  segmented-control:   { backgroundColor: "{colors.tint-1}", border: "1px solid {colors.hairline}", rounded: "{rounded.md}", padding: 2px, itemSelected: "--hsc-seg-on-bg + 1px inset {colors.hairline-strong} + --hsc-seg-on-shadow, {colors.ink} at 600, icon in {colors.primary} (light: {colors.primary-text-light})" }
  status-pill:         { backgroundColor: "<status color> at 15%", textColor: "<status color>", typography: "{typography.caption} 500", rounded: "{rounded.pill}", padding: "2px 8px" }
  status-dot:          { size: 8px, rounded: "{rounded.pill}" }
  filter-chip:         { backgroundColor: transparent, border: "1px solid {colors.hairline}", rounded: "{rounded.pill}", padding: "4px 12px", selected: "{colors.tint-3} + {colors.ink}" }
  text-input:          { note: "Use Emby's own emby-input / emby-select / emby-checkbox. Do not restyle them." }
  info-box:            { backgroundColor: "{colors.tint-1}", border: "1px solid {colors.hairline}", rounded: "{rounded.sm}", padding: "{spacing.sm}", typography: "{typography.body-sm}" }
  callout:             { backgroundColor: "<semantic color> at 8%", border: "1px solid <semantic color> at 30%", rounded: "{rounded.sm}", padding: "{spacing.sm}" }
  modal:               { backgroundColor: "{colors.popup-bg}", textColor: "{colors.popup-ink}", border: "1px solid {colors.popup-hairline}", rounded: "{rounded.lg}", shadow: "0 24px 48px rgba(0,0,0,0.45)", maxWidth: 650px }
  modal-header:        { backgroundColor: transparent, borderBottom: "1px solid {colors.popup-hairline}", padding: "16px 20px", iconColor: "{colors.primary}", typography: "{typography.section-title}" }
  list-row:            { padding: "8px 12px", rounded: "{rounded.sm}", hover: "{colors.tint-2}", divider: "1px solid {colors.hairline}" }
  log-panel:           { backgroundColor: "{colors.log-bg}", textColor: "{colors.log-ink}", typography: "{typography.mono}", rounded: "{rounded.md}", padding: "{spacing.sm}" }
---

# Home Screen Companion — Design

Based on Linear's design language (dense, calm, one accent, depth through surfaces instead of shadows), adapted to live **inside Emby's dashboard** with Emby's colours. The plugin page must always look like it belongs to Emby: it inherits Emby's font, form controls and theme (dark *and* light). What we take from Linear is the restraint.

## Principles

1. **Emby owns the frame.** Font, inputs, selects, checkboxes, toggles and the page background come from Emby. Use `emby-input`, `emby-select`, `emby-checkbox`, `emby-button` (`raised`). Never set a `font-family` and never restyle Emby's form controls.
2. **Theme-relative, never hard-coded surfaces.** Every background, border and text colour uses Emby's theme variables (`--theme-background-level1..3`, `--line-color`, `--theme-text-primary/secondary`) or neutral grey tints (`rgba(128,128,128,a)`), which work in both themes. Popups use the `--plugin-popup-*` variables set in `configPage.js`. Only accent and semantic colours are fixed hex values.
3. **One accent, used sparingly.** Emby green marks *the* primary action (Save), the active tab, focus, enabled states and success. It is not a decoration colour: no green section titles, no green gradient headers, no green left-borders on every row.
4. **Blue means "something is happening".** Emby blue is reserved for running/in-progress state, the Run action and live activity. It is not a second decorative accent.
5. **Depth = surface ladder + hairlines.** canvas → surface-1 → surface-2, each with a 1px `--line-color` border. Shadows only on things that float (modals, FABs, dropdowns), plus the small lift shadow on the selected item of a segmented control.
6. **Quiet type.** Sentence case for titles, tabs and buttons. Weight 600 for headings, 500 for labels and buttons, 400 for body. UPPERCASE is reserved for the small `eyebrow` style (group labels above a list), never for tabs or section titles.
7. **Density with air.** A 4px grid. 24px inside cards, 16px between related controls, 24–32px between cards.

## Colours

### Accents
| Token | Value | Use |
|---|---|---|
| `primary` | `#52B54B` | Save button, active tab underline, focus ring, toggles on, success |
| `primary-hover` | `#5fc458` | Hover on primary buttons |
| `primary-pressed` | `#3a8c34` | Pressed state |
| `primary-text-light` | `#3a8c34` | Green **text or icons** on the light theme (contrast) |
| `activity` | `#00a4dc` | Run button, running status dot/pill, progress bars, live log tab |
| `activity-text-light` | `#007ba8` | Blue text on the light theme |

### Semantic
| Token | Value | Use |
|---|---|---|
| `success` | `#52B54B` | Completed, added, enabled |
| `warning` | `#e6c15a` | Warnings, partial results, "needs attention" |
| `danger` | `#e57373` | Errors, removed items, destructive text |
| `danger-strong` | `#cc3333` | Failed status dot, confirm-delete button |
| `neutral` | `#9aa4b0` | Skipped, disabled, not configured |

Status colours are always shown as **tinted pills** (colour at 15% as background, full colour as text) or as an **8px dot**. Never fill large areas with them.

Consolidation: the page currently uses several near-duplicates (`#e0a800`, `#e8a838`, `#dc7800` for warning; `#ef6e6e`, `#cc3333`, `#c62828`, `#b71c1c` for red; `#3a9c34`, `#3a8c34`, `#3d8a38` for dark green). New code uses only the tokens above. Fold the old values in when touching that code.

### Surfaces and text
| Token | Value | Use |
|---|---|---|
| `canvas` | `--theme-background-level1` | Page; bodies of expanded rows |
| `surface-1` | `--theme-background-level2` | Cards, collapsible rows |
| `surface-2` | `--theme-background-level3` | Hovered headers |
| `tint-1/2/3` | grey at 4% / 8% / 15% | Header strips / hover / selected |
| `hairline` | `--line-color` | All 1px borders and dividers |
| `ink` | `--theme-text-primary` | Titles, body |
| `ink-muted` | `--theme-text-secondary` | Descriptions, meta, inactive tabs |

## Typography

The font is Emby's. Sizes are in `em` so they follow Emby's scaling.

| Token | Size | Weight | Use |
|---|---|---|---|
| `page-title` | 1.6em | 600 | Page heading (once) |
| `section-title` | 1.05em | 600 | Card headings, modal titles |
| `card-title` | 1em | 600 | Row titles (tag, collection, section) |
| `body` | 0.95em | 400 | Default text |
| `body-sm` | 0.88em | 400 | Field descriptions, info boxes |
| `caption` | 0.8em | 400 | Meta, timestamps, pill text |
| `label` | 0.88em | 500 | Field labels |
| `tab` / `button` | 0.9em | 500 | Tabs, buttons |
| `eyebrow` | 0.75em | 600, +0.6px, UPPERCASE | Small group labels only |
| `mono` | 0.85em | 400 | Logs, IDs, URLs, file paths |

Headings get a slight negative tracking (-0.1 to -0.3px); everything else 0.

## Layout and shape

- Spacing grid: 4 / 8 / 12 / 16 / 24 / 32 px.
- Radius: 4px (chips inside rows, small tags), 6px (info boxes, list rows), **8px (cards, buttons, rows, FABs)**, 12px (modals), pill (status pills, filter chips).
- No pill-shaped action buttons. Pills are only for status and filters.
- Card grids use `auto-fill, minmax(…, 1fr)` and collapse to one column on phones; the page must work at 360px width.

## Elevation

| Level | Treatment | Use |
|---|---|---|
| 0 | none | Page, text |
| 1 | `surface-1` + 1px `hairline` | Cards, collapsible rows |
| 2 | `surface-2` + 1px `hairline-strong` | Hovered or expanded headers |
| Float | shadow `0 4px 16px rgba(0,0,0,.35)` | FABs, dropdowns, speed-dial menu |
| Modal | shadow `0 24px 48px rgba(0,0,0,.45)` + backdrop `rgba(0,0,0,.6)` | Dialogs |
| Focus | 2px `primary` outline at 50%, 2px offset | Keyboard focus on custom controls |

Cards and rows have **no** drop shadow.

## Components

### Buttons
- **Primary** (Save): green fill, white text, 8px radius. One per view.
- **Activity** (Run now / Sync): blue fill. Only for starting a job.
- **Secondary**: grey tint fill, hairline border, ink text. Default for everything else (Add, Import, Test).
- **Ghost**: no fill, muted text, for icon buttons and inline actions (Edit, Duplicate, ⋯).
- **Danger**: red text with a red 40% border; it fills `danger-strong` only in the confirm step.
- **FAB** (floating Save/Run): 44px high, 8px radius, float shadow.

### Tabs
- **Page tabs and sub-tabs**: sentence case, weight 500, `ink-muted`. The active tab gets `ink` text and a 2px green underline. Hover gives `ink` text with no underline. A 1px `hairline` sits under the whole bar.
- **Segmented control** (small 2–4 choice toggles): a tinted track with a 2px inset. The selected item is lifted above the track: `--hsc-seg-on-bg` (white at 10% in dark, plain white in light), a 1px inset `hairline-strong` ring and `--hsc-seg-on-shadow` (0 1px 2px). Its text is `ink` at weight 600 and only its icon is Emby green (`primary`, `primary-text-light` on the light theme, via `--hsc-primary-text`). Unselected items are transparent at 70% opacity. Never fill the selected item with green. A choice that switches between lists or views is a sub-tab row, not a segmented control.

### Cards and collapsible rows
- **Card**: `surface-1`, hairline border, 8px radius, 24px padding. The title is `section-title` in `ink`; an optional `ink-muted` one-line description sits under it.
- **Collapsible row** (tags, collections, home sections): header 12px × 16px on `surface-1`. From left to right: chevron, title (`card-title`), meta (`caption`, `ink-muted`), then right-aligned status pill and actions. The body sits on `canvas` with a hairline top border. Enabled/disabled is shown by the status pill or dot, not by a coloured left border. A disabled row drops to 60% opacity on the title.

### Status
- **Status pill**: `caption` at weight 500, pill radius, 2px × 8px. Values: Active (green), Running (blue), Warning (yellow), Failed (red), Disabled/Skipped (neutral).
- **Status dot**: 8px. Running pulses slowly (1.5s).
- **Progress bar**: 4px high, `tint-2` track, `activity` fill, pill radius.

### Lists
- **List row**: 8px × 12px, 6px radius, `tint-2` on hover. Rows are separated by spacing, or by a hairline in dense tables.
- **Type chip** (Tag, Collection, Playlist, Top-list, Home section, Schedule, Imported, Movies/Shows on a row header): neutral — hairline border, `ink-muted` text, 4px radius, `caption` size. Only the icon carries its `cat-*` colour (Home section uses `primary`, Schedule uses `activity`), so types can be told apart at a glance without colouring the chip.
- **Filter chip**: a pill with a hairline border; when selected it gets the `tint-3` fill and `ink` text. An icon may carry semantic colour; the chip itself never does.
- **Empty state**: centred, `ink-muted`, one line of text plus one secondary button. No illustrations.

### Feedback
- **Info box**: `tint-1`, hairline, 6px radius, `body-sm`.
- **Callout** (warning, success, error): the semantic colour at 8% background with a 30% border. The icon takes the semantic colour; the text stays `ink`.
- **Toast**: use Emby's own `Dashboard.alert` or toast. Don't build a custom one.

### Modals
- `popup-bg`, `popup-hairline` border, 12px radius, modal shadow, max-width 650px.
- **Header**: transparent with a hairline bottom border. The icon is in `primary`, the title is `section-title` in `popup-ink`, and a ghost close button sits on the right. **No gradient header.**
- **Footer**: right-aligned buttons with secondary first and the primary action last. A hairline sits above it.

### Log viewer
- Always dark (`log-bg`) whatever the theme, with `mono` text and the log-* colours for line types.
- **Log tabs**: filter chips. The active tab gets the `activity` fill.

## Do and don't

**Do**
- Use Emby's theme variables for every surface, border and text colour.
- Keep green for the primary action, the active state, focus and success.
- Show state with small pills and dots.
- Test every change in both Emby's dark and light themes.
- Use `primary-text-light` / `activity-text-light` when green or blue is used as text on the light theme.

**Don't**
- Don't set fonts or restyle Emby's inputs.
- Don't put drop shadows on cards or rows.
- Don't use gradient headers, coloured section titles or coloured row borders.
- Don't use UPPERCASE tabs or titles.
- Don't put more than one green filled button in view at once.
- Don't introduce new hex colours. Add a token here first.

## Migration notes (current → target)

| Current | Target |
|---|---|
| `.page-tab-btn`, `.hsc-sub-tab-btn`, `.tc-sub-tab-btn`: uppercase, green text when active | Sentence case, `ink` text with a 2px green underline. Merge them into one tab class. |
| `.hsc-section-title`: green, uppercase, tracked | `section-title` in `ink`. Use `eyebrow` only for small group labels. |
| `.tag-row`: 5px green left border and a drop shadow | Hairline only. Show enabled state with a status pill. |
| `.modal-header-bar`: green gradient | Transparent header with a hairline and a green icon |
| `.fab-btn`: 25px pill radius | 8px radius, 44px high |
| Scattered warning and red hex values | The `warning` / `danger` / `danger-strong` tokens |

Migrate one component at a time, and check each one in both themes on the LAN test server.
