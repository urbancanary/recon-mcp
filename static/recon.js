/* Fund Reconciliation UI — renders GET /aum/{fund}.
 *
 * Conventions carried from the Athena widget it replaces:
 *   - null renders as an em-dash, NEVER 0.00 (a zero is a claim);
 *   - not_like_for_like diffs are muted, never red — a non-zero there is a
 *     definitional difference, not a break;
 *   - the source filenames are always visible: every number on this page is
 *     an assertion about two specific files.
 */
(function () {
    'use strict';

    const $ = (id) => document.getElementById(id);
    const state = { fund: null, date: null, funds: [] };

    // ── formatting ──────────────────────────────────────────────────────
    const DASH = '—';
    function num(v, dp = 2) {
        if (v === null || v === undefined || Number.isNaN(v)) return DASH;
        return Number(v).toLocaleString('en-GB',
            { minimumFractionDigits: dp, maximumFractionDigits: dp });
    }
    function diffCell(v, mutedFlag) {
        if (v === null || v === undefined) return `<td>${DASH}</td>`;
        const cls = mutedFlag || Math.abs(v) < 0.01 ? 'num-mut'
            : (v < 0 ? 'num-neg' : 'num-pos');
        return `<td class="${cls}">${num(v)}</td>`;
    }
    function esc(s) {
        return String(s ?? '').replace(/[&<>"]/g,
            c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
    }

    async function jget(url) {
        const r = await fetch(url, { credentials: 'same-origin' });
        if (r.status === 401) throw new Error('Not logged in — refresh to sign in.');
        if (!r.ok) throw new Error((await r.json().catch(() => ({}))).detail || `HTTP ${r.status}`);
        return r.json();
    }

    // ── sections ────────────────────────────────────────────────────────
    function renderVerdict(r) {
        const i = r.integrity;
        if (!i) { $('verdict').innerHTML = ''; return; }
        $('verdict').innerHTML = `<div class="banner ${i.material ? 'bad' : 'ok'}">
            <b>${i.material ? 'MATERIAL DIFFERENCE' : 'AGREES'}</b> —
            Maia ${num(i.maia_total)} vs administrator ${num(i.admin_nav)}:
            ${num(i.difference)} (${i.difference_pct > 0 ? '+' : ''}${i.difference_pct}%).
            <div style="font-size:12.5px;margin-top:4px;opacity:.9">${esc(i.verdict)}</div></div>`;
    }

    function renderDateWarning(r) {
        const m = r.meta;
        $('dateWarning').innerHTML = m.dates_match ? '' :
            `<div class="banner warn">Dates differ — administrator ${esc(m.valuation_date)}
             vs Maia ${esc(m.maia_valuation_date || DASH)}. Every difference below includes
             market and FX movement between the two snapshots.</div>`;
    }

    function renderAum(r) {
        const rows = (r.rows || []).map(row => `<tr>
            <td class="lbl">${esc(row.label)}${row.not_like_for_like
                ? '<span class="badge">not like-for-like</span>' : ''}</td>
            ${diffCell(row.diff_maia_athena, row.not_like_for_like)}
            <td>${num(row.maia)}</td><td>${num(row.athena)}</td><td>${num(row.waystone)}</td>
            ${diffCell(row.diff_athena_waystone, true)}
            ${diffCell(row.diff_maia_waystone, row.not_like_for_like)}</tr>`).join('');
        const t = r.total || {};
        const memo = (r.memo_rows || []).map(mr => `<tr class="memo">
            <td class="lbl">${esc(mr.label)} <span class="muted">${esc(mr.note || '')}</span></td>
            <td>${DASH}</td><td>${num(mr.maia)}</td><td>${num(mr.athena)}</td>
            <td>${num(mr.waystone)}</td><td>${DASH}</td><td>${DASH}</td></tr>`).join('');
        $('aumTable').innerHTML = `<table><thead><tr>
            <th>Component</th><th>&Delta; Maia&minus;Athena</th><th>Maia</th>
            <th>Athena</th><th>Waystone</th><th>&Delta; Athena&minus;Waystone</th>
            <th>&Delta; Maia&minus;Waystone</th></tr></thead><tbody>${rows}
            <tr class="total"><td class="lbl">${esc(t.label || 'TOTAL NAV')}</td>
            ${diffCell(t.diff_maia_athena)}<td>${num(t.maia)}</td><td>${num(t.athena)}</td>
            <td>${num(t.waystone)}</td>${diffCell(t.diff_athena_waystone, true)}
            ${diffCell(t.diff_maia_waystone)}</tr>${memo}</tbody></table>`;
        $('notes').innerHTML = (r.notes || [])
            .map(n => `<div class="note">&#9888; ${esc(n)}</div>`).join('');
        const m = r.meta || {};
        $('sources').textContent =
            `admin: ${m.admin_file || DASH} @ ${m.valuation_date || DASH}` +
            (m.valuation_time ? ` ${m.valuation_time}` : '') +
            ` | maia: ${(m.maia || {}).file || 'none'} @ ${m.maia_valuation_date || DASH}` +
            ` | base ${m.base_currency || ''}`;
    }

    function renderEvidence(r) {
        const e = r.evidence_report;
        if (!e || e.error) {
            $('evidence').innerHTML = `<div class="muted">${esc((e || {}).error || 'unavailable')}</div>`;
            return;
        }
        const pre = (e.preconditions || []).map(p => {
            const b = p.passes === true ? 'pass' : p.passes === false ? 'fail' : '';
            const t = p.passes === true ? 'pass' : p.passes === false ? 'FAIL' : 'n/a';
            return `<tr><td class="lbl">${esc(p.condition)}</td>
                <td style="text-align:left"><span class="badge ${b}">${t}</span>
                <span class="muted">${esc(p.note || '')}</span></td></tr>`;
        }).join('');
        const findings = (e.findings || []).map(f => `<div class="finding">
            <b>#${f.n} ${esc(f.finding)}</b><span class="badge ${esc(f.status)}">${esc(f.status)}</span>
            <div class="mag">${esc(f.magnitude || '')}</div>
            <div>${esc(f.evidence || '')}</div>
            <div class="muted">${esc(f.detail || '')}</div>
            ${f.not_this ? `<div class="muted">Not: ${esc(f.not_this)}</div>` : ''}</div>`).join('');
        const nt = (e.not_tested || []).map(x =>
            `<li><b>${esc(x.heading)}</b> — ${esc(x.detail)}</li>`).join('');
        $('evidence').innerHTML = `
            <div style="margin-bottom:8px">${esc((e.scope || {}).affects || '')}</div>
            <table style="margin-bottom:12px"><tbody>${pre}</tbody></table>
            ${findings}
            ${nt ? `<h2 style="margin-top:14px">Not tested</h2><ul class="muted">${nt}</ul>` : ''}
            <div class="src" style="margin-top:8px">${esc(e.basis || '')}</div>`;
    }

    function renderAttribution(r) {
        const a = r.attribution;
        if (!a || !a.available) {
            $('attribution').innerHTML = `<div class="muted">${esc((a || {}).error || 'unavailable')}</div>`;
            return;
        }
        const t = a.totals || {};
        $('attribution').innerHTML = `<table><thead><tr>
            <th>Pass</th><th>Par</th><th>Price</th><th>FX</th><th>Accrued</th><th>Residual</th>
            </tr></thead><tbody>
            <tr><td class="lbl">Own marks (exact decomposition)</td>
                ${diffCell(t.par)}${diffCell(t.price)}${diffCell(t.fx)}${diffCell(t.accrued)}
                <td>${num(a.decomposition_residual)}</td></tr>
            <tr><td class="lbl">Maia at admin marks (par only)</td>
                <td colspan="4">${num(t.maia_mv_at_admin_marks)}</td>
                <td>${num(a.residual_at_admin_marks)}</td></tr>
            <tr><td class="lbl">Dirty at admin marks (accrual convention)</td>
                <td colspan="4">${DASH}</td><td>${num(t.dirty_residual)}</td></tr>
            </tbody></table>
            <div class="src">reference: ${esc(a.reference_file || DASH)} |
            admin ${esc(a.admin_date || DASH)} vs maia ${esc(a.maia_date || DASH)}
            ${a.dates_match ? '' : '<span class="err">(dates differ)</span>'}</div>`;
    }

    function renderBonds(r) {
        const b = r.bonds;
        if (!b) { $('bonds').innerHTML = '<div class="muted">per-bond join unavailable</div>'; return; }
        let warn = (b.warnings || []).map(w => `<div class="note">&#9888; ${esc(w)}</div>`).join('');
        if (b.bridge_stale) {
            warn += `<div class="note err">Ticker&rarr;ISIN bridge is dated ${esc((b.bridge || {}).as_of)}
                but the priced view is ${esc(b.priced_as_of)} — position-break and coverage
                figures are suppressed (a stale bridge reports absence as fact).</div>`;
        }
        const rows = (b.rows || []).map(x => `<tr>
            <td class="lbl">${esc(x.isin)} <span class="muted">${esc(x.description || '')}</span></td>
            <td>${num(x.admin_par, 0)}</td><td>${num(x.maia_par, 0)}</td>
            ${diffCell(x.par_diff)}
            <td>${num(x.admin_price, 4)}</td><td>${num(x.maia_price, 4)}</td>
            <td>${x.price_diff_bp == null ? DASH : num(x.price_diff_bp, 1)}</td>
            <td>${num(x.admin_dirty_base)}</td><td>${num(x.maia_exposure_base)}</td></tr>`).join('');
        const only = (lst, who) => (lst || []).length
            ? `<div class="note">${who} only: ${lst.map(esc).join(', ')}</div>` : '';
        $('bonds').innerHTML = warn + `<table><thead><tr>
            <th>Bond</th><th>Admin par</th><th>Maia par</th><th>&Delta; par</th>
            <th>Admin px</th><th>Maia px</th><th>&Delta; bp</th>
            <th>Admin dirty</th><th>Maia exposure</th></tr></thead>
            <tbody>${rows}</tbody></table>`
            + (b.bridge_stale ? '' : only(b.admin_only, 'Administrator')
                + only(b.maia_only, 'Maia'));
    }

    function renderPrices(r) {
        const p = r.prices || {};
        const marks = r.athena_marks || {};
        const rows = (p.rows || []).map(x => {
            const ga = marks[x.isin] || {};
            return `<tr><td class="lbl">${esc(x.isin)}</td><td>${esc(x.currency || '')}</td>
                <td>${num(x.price_waystone, 4)}</td><td>${num(x.price_maia, 4)}</td>
                <td>${num(ga.clean_price, 4)}</td>
                <td>${x.price_diff_maia_waystone == null ? DASH
                    : num(x.price_diff_maia_waystone * 100, 1)}</td></tr>`;
        }).join('');
        $('prices').innerHTML = `<table><thead><tr><th>ISIN</th><th>Ccy</th>
            <th>Waystone</th><th>Maia</th><th>GA10</th><th>M&minus;W bp</th></tr></thead>
            <tbody>${rows}</tbody></table>
            <div class="src">repriced at constant administrator par —
            totals W ${num((p.totals || {}).waystone)} / M ${num((p.totals || {}).maia)}</div>`;
    }

    function renderAccrued(r) {
        const marks = r.athena_marks || {};
        const b = r.bonds;
        const rows = ((b && b.rows) || []).map(x => {
            const ga = marks[x.isin] || {};
            return `<tr><td class="lbl">${esc(x.isin)}</td>
                <td>${num(x.admin_accrued_base)}</td>
                <td>${num(ga.accrued_per_100, 4)}</td></tr>`;
        }).join('');
        $('accrued').innerHTML = rows
            ? `<table><thead><tr><th>ISIN</th><th>Admin accrued (base)</th>
               <th>GA10 accrued /100</th></tr></thead><tbody>${rows}</tbody></table>
               ${Object.keys(marks).length ? '' :
                 '<div class="muted">GA10 marks unavailable for this book — admin column only.</div>'}`
            : '<div class="muted">needs the per-bond join</div>';
    }

    function renderFx(r) {
        const maiaFx = ((r.meta || {}).maia || {}).fx_rates || {};
        const a = r.attribution || {};
        const adminFx = a.admin_fx || {};
        const ccys = [...new Set([...Object.keys(adminFx), ...Object.keys(maiaFx)])].sort();
        if (!ccys.length) { $('fx').innerHTML = '<div class="muted">no non-base currencies</div>'; return; }
        $('fx').innerHTML = `<table><thead><tr><th>Ccy</th><th>Waystone</th><th>Maia</th>
            <th>&Delta;</th></tr></thead><tbody>` + ccys.map(c => {
                const w = adminFx[c], m = maiaFx[c];
                const d = (w != null && m != null) ? m - w : null;
                return `<tr><td class="lbl">${esc(c)}</td><td>${num(w, 6)}</td>
                    <td>${num(m, 6)}</td>${diffCell(d)}</tr>`;
            }).join('') + '</tbody></table>';
    }

    function renderCurrency(r) {
        const c = r.currency;
        if (!c || c.error) {
            $('currency').innerHTML = `<div class="muted">${esc((c || {}).error || 'unavailable')}</div>`;
            return;
        }
        const exp = (c.exposure || c.rows || []).map(x => `<tr>
            <td class="lbl">${esc(x.currency || x.ccy || '')}</td>
            <td>${num(x.exposure ?? x.exposure_base)}</td>
            <td>${num(x.hedge ?? x.hedge_notional)}</td>
            <td>${x.coverage_pct == null ? DASH : num(x.coverage_pct, 1) + '%'}</td></tr>`).join('');
        const cls = (c.share_classes || []).map(x => `<tr>
            <td class="lbl">${esc(x.share_class || x.name || '')}</td>
            <td>${num(x.hedge_pl ?? x.pl)}</td>
            <td>${x.per_share == null ? DASH : num(x.per_share, 5)}</td></tr>`).join('');
        $('currency').innerHTML =
            (exp ? `<table style="margin-bottom:10px"><thead><tr><th>Ccy</th><th>Exposure</th>
                <th>Hedge</th><th>Coverage</th></tr></thead><tbody>${exp}</tbody></table>` : '')
            + (cls ? `<table><thead><tr><th>Share class</th><th>Hedge P&amp;L</th>
                <th>Per share</th></tr></thead><tbody>${cls}</tbody></table>` : '')
            + (!exp && !cls ? `<pre class="muted" style="white-space:pre-wrap">${esc(JSON.stringify(c, null, 1))}</pre>` : '');
    }

    async function renderUploadHistory() {
        try {
            const h = await jget(`/aum/${state.fund}/uploads`);
            const rows = (h.uploads || []).slice(0, 20).map(u => `<tr>
                <td class="lbl">${esc(u.source)}</td><td>${esc(u.date)}</td>
                <td style="text-align:left">${esc(u.file_name)}</td>
                <td style="text-align:left">${esc(u.uploaded_by || '')}</td>
                <td>${esc((u.uploaded_at || '').slice(0, 16).replace('T', ' '))}</td>
                <td style="text-align:left">${esc(u.parse_status)}${u.parse_error
                    ? ` <span class="err">${esc(u.parse_error)}</span>` : ''}</td></tr>`).join('');
            $('uploadHistory').innerHTML = rows
                ? `<table><thead><tr><th>Source</th><th>Data date</th><th>File</th>
                   <th>By</th><th>Uploaded</th><th>Status</th></tr></thead><tbody>${rows}</tbody></table>`
                : '<div class="muted">no uploads on record</div>';
        } catch (e) {
            $('uploadHistory').innerHTML = `<div class="err">${esc(e.message)}</div>`;
        }
    }

    // ── uploads ─────────────────────────────────────────────────────────
    function wireDrop(dropId, inputId, statusId, dest) {
        const drop = $(dropId), input = $(inputId), status = $(statusId);
        drop.addEventListener('click', () => input.click());
        ['dragover', 'dragenter'].forEach(ev => drop.addEventListener(ev, e => {
            e.preventDefault(); drop.classList.add('armed');
        }));
        ['dragleave', 'drop'].forEach(ev => drop.addEventListener(ev, e => {
            e.preventDefault(); drop.classList.remove('armed');
        }));
        drop.addEventListener('drop', e => { if (e.dataTransfer.files[0]) send(e.dataTransfer.files[0]); });
        input.addEventListener('change', () => { if (input.files[0]) send(input.files[0]); });

        async function send(file) {
            status.innerHTML = `<span class="muted">Uploading ${esc(file.name)}…</span>`;
            const fd = new FormData();
            fd.append('file', file, file.name);
            try {
                const url = dest === 'maia'
                    ? `/aum/upload/maia?fund=${encodeURIComponent(state.fund)}`
                    : '/upload/admin';
                const r = await fetch(url, { method: 'POST', body: fd, credentials: 'same-origin' });
                const body = await r.json().catch(() => ({}));
                if (!r.ok) throw new Error(body.detail || `HTTP ${r.status}`);
                status.innerHTML = `<span class="num-pos">Stored as ${esc(body.date
                    || body.portfolio_id || 'ok')}${body.bonds ? ` (${body.bonds} bonds)` : ''}</span>`;
                await loadDates();
                await load();
            } catch (e) {
                status.innerHTML = `<span class="err">${esc(e.message)}</span>`;
            }
        }
    }

    // ── loading ─────────────────────────────────────────────────────────
    async function loadFunds() {
        const f = await jget('/funds');
        state.funds = (f.funds || []).filter(x => x.aum);
        $('fund').innerHTML = state.funds.map(x =>
            `<option value="${x.id}">${esc(x.name)}</option>`).join('');
        state.fund = state.funds[0] && state.funds[0].id;
        const hashFund = (location.hash.match(/fund=([a-z0-9_]+)/) || [])[1];
        if (hashFund && state.funds.some(x => x.id === hashFund)) state.fund = hashFund;
        $('fund').value = state.fund;
    }

    async function loadDates() {
        const d = await jget(`/aum/${state.fund}/dates`);
        const opts = (d.maia_dates || []).map(x =>
            `<option value="${x}">${x}${(d.matched_dates || []).includes(x)
                ? '' : ' (no same-day admin pack)'}</option>`);
        $('datePair').innerHTML = opts.join('') || '<option value="">no Maia exports</option>';
        state.date = $('datePair').value || null;
    }

    async function load() {
        $('status').textContent = 'loading…';
        try {
            const url = `/aum/${state.fund}` + (state.date ? `?date=${state.date}` : '');
            const r = await jget(url);
            renderVerdict(r); renderDateWarning(r); renderAum(r); renderEvidence(r);
            renderAttribution(r); renderBonds(r); renderPrices(r); renderAccrued(r);
            renderFx(r); renderCurrency(r);
            $('status').textContent = '';
            renderUploadHistory();
        } catch (e) {
            $('status').innerHTML = `<span class="err">${esc(e.message)}</span>`;
        }
    }

    $('fund').addEventListener('change', async () => {
        state.fund = $('fund').value;
        location.hash = `fund=${state.fund}`;
        await loadDates(); load();
    });
    $('datePair').addEventListener('change', () => { state.date = $('datePair').value || null; load(); });
    $('reload').addEventListener('click', load);

    (async function init() {
        try {
            await loadFunds();
            await loadDates();
            wireDrop('dropMaia', 'fileMaia', 'maiaUpStatus', 'maia');
            wireDrop('dropAdmin', 'fileAdmin', 'adminUpStatus', 'admin');
            await load();
        } catch (e) {
            document.body.insertAdjacentHTML('afterbegin',
                `<div class="banner bad">${esc(e.message)}</div>`);
        }
    })();
})();
