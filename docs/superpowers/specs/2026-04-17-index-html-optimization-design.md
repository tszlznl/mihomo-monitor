# Index HTML Optimization Design

**Date:** 2026-04-17

## Goal

Optimize the embedded web UI so it is easier to maintain and easier to use, without changing the product model of "single binary + embedded static files".

## Context

The current UI works and already covers the required behavior:

- Mihomo settings management
- Time range selection
- Trend chart
- Three-level drilldown
- Manual refresh
- Data clearing

The current implementation cost is mostly in maintainability. The entire page lives in a single [`web/index.html`](../../../web/index.html) file containing HTML, CSS, and JavaScript together. That makes visual polish, structural changes, and behavior changes harder than they need to be.

## Non-Goals

This optimization will not:

- introduce a frontend framework
- introduce a bundler or build pipeline
- change backend API semantics
- change database behavior
- redesign the product into multiple pages
- add complex new filtering or reporting features

## Constraints

- Keep deployment exactly as it is today: static files served by Go embed.
- Preserve the existing feature set.
- Keep the page usable on desktop and small screens.
- Avoid over-engineering; the project remains a lightweight LAN-oriented tool.

## Current Problems

### 1. File boundary is poor

HTML, CSS, and JavaScript are coupled inside one large file. That makes local edits slower and increases regression risk.

### 2. Information hierarchy is weaker than it should be

The page contains the right data, but the structure is not explicit enough about:

- what is global control
- what is status feedback
- what is overview
- what is drilldown context
- what is modal/setup flow

### 3. Drilldown context is too implicit

When a user selects a primary or secondary row, the next region updates, but the page does not strongly communicate the current drill path.

### 4. Visual consistency can be tightened

The current design direction is good, but spacing, state styling, and emphasis can be made more consistent so the page feels more finished.

## Proposed Approach

### File Structure

Split the current page into three embedded static files:

- [`web/index.html`](../../../web/index.html)
  - semantic page skeleton only
  - panels, sections, containers, and static template structure
  - references to stylesheet and script
- `web/styles.css`
  - all styling rules
  - organized by variables, layout, shared components, sections, state styles, and responsive rules
- `web/app.js`
  - all runtime behavior
  - organized by state, DOM references, utilities, API helpers, renderers, chart logic, event bindings, and boot flow

This keeps the current static-serving model while removing the single-file bottleneck.

### Script Organization

`web/app.js` should be grouped into these sections:

1. App state
2. DOM element references
3. Utility functions
4. API functions
5. Settings panel flow
6. Time-range flow
7. Data loading and request sequencing
8. Trend rendering
9. Drilldown rendering
10. Status and empty-state rendering
11. Event binding
12. App bootstrap

The goal is not to simulate a framework. The goal is to make the file readable and safe to modify.

## UX Structure

The page should be organized into five clear regions.

### 1. Global Control Region

Contains:

- brand
- dimension switch
- time range controls
- refresh button
- clear button
- settings button

This area owns all page-level actions.

### 2. Status Region

Contains:

- success/error status banner
- lightweight runtime summary such as current dimension, time window, and connection state

The page should not only communicate state when an error occurs.

### 3. Overview Region

Contains:

- trend chart as the first visual focus

This region should visually communicate that the user first sees the traffic overview, then drills into ranked detail.

### 4. Drilldown Region

Contains:

- primary ranking panel
- secondary ranking panel
- connection detail panel

The structure should make the drill path explicit. Titles and helper text should change based on the current dimension and selection state.

Examples:

- Device Ranking
- Hosts Accessed by 192.168.1.10
- Proxies Used for a.com
- Connection Details for a.com / 192.168.1.10

### 5. Settings Region

Contains:

- Mihomo setup/update form

It should remain a distinct setup surface rather than blending into the dashboard.

## Visual Direction

Keep the current general style:

- light theme
- soft glass panels
- atmospheric gradient background

Refine rather than replace it:

- make spacing scale more consistent
- unify button sizes and panel padding
- tighten heading hierarchy
- improve table readability
- strengthen selected-row state
- make empty/loading states clearer
- keep the trend chart as the strongest visual anchor

## Interaction Changes

### Selection Feedback

Primary and secondary selected states should become more visually explicit.

### Contextual Titles

Each panel title and helper line should reflect the current dimension and selection state instead of using generic wording.

### Empty States

Differentiate these cases:

- settings required
- no data in selected time range
- no secondary data for current selection
- no detail rows for current selection
- request failure

### Loading Feedback

Refresh, save, and clear actions should consistently update button disabled states and status messaging so repeated clicks are discouraged.

### Small-Screen Behavior

The page should remain usable on narrow widths without breaking control layout or making tables unreadable.

## Technical Notes

- Continue serving static files through Go embed.
- Update tests so they validate the new embedded asset layout.
- Preserve route compatibility for `/`, `/favicon.svg`, and other embedded assets.
- Do not add external dependencies.

## Acceptance Criteria

The work is complete when all of the following are true:

- the UI is split into `index.html`, `styles.css`, and `app.js`
- the Go server still serves the embedded UI correctly
- all existing user-facing flows still work
- information hierarchy is clearer than before
- drilldown selection context is visually obvious
- status, loading, and empty states are more explicit
- the page remains responsive on smaller screens
- no frontend build step is introduced

## Verification

Verification should cover:

- page boot when Mihomo settings are missing
- page boot when Mihomo settings are already saved
- settings save flow
- refresh flow
- clear flow
- dimension switching
- time range switching
- primary to secondary to detail drilldown
- embedded asset serving tests in Go

## Implementation Boundary

This work should primarily modify:

- [`web/index.html`](../../../web/index.html)
- `web/styles.css`
- `web/app.js`
- relevant Go tests that assert embedded UI content or asset serving

Backend logic should only be touched if required to keep embedded asset serving correct.
