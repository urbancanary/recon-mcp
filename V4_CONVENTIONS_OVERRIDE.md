# V4 Conventions Override Pipeline

## Summary

There is a new conventions override pipeline that improves bond analytics accuracy. It resolves bond static data (day_count, frequency, coupon, maturity, issue_date, first_coupon_date) from Supabase `bond_identity` and sends them as `overrides` to the GA10 backend. This ensures consistent correct results regardless of whether CBonds is available.

**Athena stays on v3 (no overrides) for now.** The recon pages should use the override pipeline to compare results and flag discrepancies.

## How it works

### Current (v3) flow
```
Request → GA10 backend → looks up ISIN in local DB → if on watchlist, fetches CBonds → calculates
```
Problem: local watchlist can be stale, CBonds can be down, local DB may not have the bond.

### New (v4) flow
```
Request + overrides from Supabase → GA10 backend → uses overrides directly → calculates
```
Overrides always win over local DB lookups. Conventions come from a 5-source hierarchy:
1. **Supabase `bond_identity`** (canonical, highest priority)
2. **CBonds MCP** (live fetch)
3. **D1 `etf_watchlist`** (coupon/maturity only)
4. **Supabase `bond_reference`** (day_count/issue_date)
5. **Currency-based defaults** (CNY = ACT/365 Annual, USD = 30/360 Semi)

## For recon pages

### Step 1: Fetch conventions from bond_identity

```javascript
// Batch fetch for all ISINs being reconciled
const resp = await fetch(
  `https://xdgicslrdudsqlsudsgv.supabase.co/rest/v1/bond_identity?isin=in.(${isins.map(i => `"${i}"`).join(',')})&select=isin,coupon,maturity_date,day_count,frequency,issue_date,first_coupon_date`,
  { headers: { apikey: BOND_DATA_KEY, Authorization: `Bearer ${BOND_DATA_KEY}` } }
);
const identityMap = Object.fromEntries((await resp.json()).map(r => [r.isin, r]));
```

### Step 2: Build overrides for bonds that have calc fields

```javascript
function buildOverrides(isin) {
  const r = identityMap[isin];
  if (!r || (!r.day_count && !r.frequency)) return null; // no calc fields, skip overrides
  
  const ov = {};
  if (r.coupon != null) ov.coupon = Number(r.coupon);
  if (r.maturity_date) ov.maturity_date = r.maturity_date;
  if (r.day_count) ov.day_count = r.day_count;
  if (r.frequency != null) ov.frequency = Number(r.frequency);
  if (r.issue_date) ov.issue_date = r.issue_date;
  if (r.first_coupon_date) ov.first_coupon_end = r.first_coupon_date;
  return ov;
}
```

### Step 3: Call GA10 with overrides

```javascript
const body = { isin, price, settlement_date };
const overrides = buildOverrides(isin);
if (overrides) body.overrides = overrides;

// Call via gateway (v2 or v3 endpoint — both accept overrides)
const result = await fetch('https://ga10-gateway.urbancanary.workers.dev/api/v2/bond/analysis', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(body)
});
```

### Step 4: Show convention source in recon

Add a column showing which convention source was used. The response includes:
- `analytics.conventions.day_count` — what was actually used for calculation
- `analytics.overrides_applied` — what overrides were sent (if any)

For the recon comparison:
- **v3 column**: call without overrides (current Athena behaviour)
- **v4 column**: call with overrides from bond_identity
- **Flag differences**: any bond where v3 != v4 has a convention discrepancy to investigate

## Key fields in bond_identity

| Column | Type | Example | Notes |
|--------|------|---------|-------|
| `day_count` | text | `ACT/365`, `30/360` | Calc-affecting |
| `frequency` | integer | `1` (annual), `2` (semi) | Calc-affecting |
| `coupon` | numeric | `2.75` | Percentage |
| `maturity_date` | date | `2027-04-26` | ISO format |
| `issue_date` | date | `2024-04-26` | For accrued calculation |
| `first_coupon_date` | date | `2025-04-26` | Send as `first_coupon_end` in overrides |

Most bonds don't have `day_count`/`frequency` populated yet. These will fill in over time as the CBonds cron processes bonds. For bonds without calc fields, don't send overrides — v3 handles them as before.

## Validation results (2026-04-07)

Tested 41 bonds (WNBF + GCRIF portfolios):
- **39 identical** between v3 and v4
- **2 improved** by v4 (bonds where backend couldn't resolve conventions)
- **0 regressions**

The two improvements:
- `XS2808428853` (KFW CNY): was using 30/360 Semi defaults, corrected to ACT/365 Annual
- `XS2492385203` (UAE): backend's local watchlist was stale, D1 watchlist now fixes v3 too
