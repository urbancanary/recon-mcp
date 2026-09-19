# Single-shot fix — recon-mcp theme:ui item 2541

**recon UI: fix currency-section renderer + per-date candidate-file comparison line**

Produced by `.cmux/ops/single_shot.py`: one no-tools call to deepseek-flash with the whole repository in a cached prefix. Transport: API. No lane ran.

- located by triage (prefix): `static/recon.js:470`
- model confidence: high — The adjudicated change is located precisely at static/recon.js:470 and requires only two small, additive edits: map the omitted accrued_base column into the currency table and surface the per-date candidate facts that build_aum_comparison must expose. The backend field is added alongside the existing Maia candidate loop without altering the newest-wins selection.
- files changed: aum_orchestrator.py, static/recon.js
- tests run (green): {"pytest": ["tests/test_aum_endpoint.py", "tests/test_candidate_views.py"]}
- prompt tokens cached / uncached: 384 / 215422; cost $0.041849

<!-- lane-result
FIXED: 2541
ALREADY_FIXED: none
DECISION: none
-->
