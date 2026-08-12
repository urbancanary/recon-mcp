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
    // ── Board brief: the landing view. Tiny renderer for the md subset the
    // server emits (h1/h2, bold, tables, ordered/unordered lists, paras) —
    // trusted input only (our own generated brief), but escaped anyway.
    function mdToHtml(md) {
        const lines = String(md || '').split('\n');
        let html = '', inUl = false, inOl = false, inTable = false;
        const closeLists = () => {
            if (inUl) { html += '</ul>'; inUl = false; }
            if (inOl) { html += '</ol>'; inOl = false; }
            if (inTable) { html += '</tbody></table>'; inTable = false; }
        };
        const inline = (s) => esc(s)
            .replace(/\*\*(.+?)\*\*/g, '<b>$1</b>')
            .replace(/`(.+?)`/g, '<code>$1</code>');
        for (const raw of lines) {
            const line = raw.trimEnd();
            if (/^\|[\s-|:]+\|$/.test(line)) continue; // separator row
            if (line.startsWith('| ') || /^\|.+\|$/.test(line)) {
                const cells = line.replace(/^\||\|$/g, '').split('|').map(c => c.trim());
                if (!inTable) {
                    closeLists();
                    html += '<table><thead><tr>' + cells.map(c =>
                        `<th>${inline(c)}</th>`).join('') + '</tr></thead><tbody>';
                    inTable = 'head';
                    continue;
                }
                html += '<tr>' + cells.map((c, ix) =>
                    `<td${ix === 0 ? ' class="lbl"' : ''}>${inline(c)}</td>`).join('') + '</tr>';
                continue;
            }
            if (inTable) { html += '</tbody></table>'; inTable = false; }
            if (line.startsWith('## ')) { closeLists(); html += `<h2>${inline(line.slice(3))}</h2>`; }
            else if (line.startsWith('# ')) { closeLists(); html += `<h1>${inline(line.slice(2))}</h1>`; }
            else if (/^\d+\.\s/.test(line)) {
                if (!inOl) { closeLists(); html += '<ol>'; inOl = true; }
                html += `<li>${inline(line.replace(/^\d+\.\s/, ''))}</li>`;
            } else if (line.startsWith('- ')) {
                if (!inUl) { closeLists(); html += '<ul>'; inUl = true; }
                html += `<li>${inline(line.slice(2))}</li>`;
            } else if (line === '') { closeLists(); }
            else { closeLists(); html += `<p>${inline(line)}</p>`; }
        }
        closeLists();
        return html;
    }

    function renderBriefing(r) {
        const md = r.briefing_md;
        const el = $('briefing');
        if (!md) { el.innerHTML = ''; return; }
        // Seriousness accent from the assessment level for the left border.
        const level = ((r.assessment || {}).level) || '';
        el.className = 'brief ' + (
            level === 'material_unexplained' || level === 'investigate' ? 'brief-high'
            : (r.fx_forward_alert || {}).alert ? 'brief-attn' : 'brief-ok');
        el.innerHTML = mdToHtml(md);
        const dl = $('downloadBrief');
        if (dl) dl.href = `/aum/${state.fund}/briefing.md`
            + (state.date ? `?date=${state.date}` : '');
    }

    // Management summary — never "AGREES" while a difference exists. The
    // headline states the gap; the cause ladder assigns every dollar of it;
    // the constant-price line says what a single price source would leave.
    const CAUSE_BADGE = {
        known_cause: ['known cause', ''],
        question_open: ['awaiting confirmation', 'indicated'],
        definitional: ['definitional', ''],
        investigate: ['INVESTIGATE', 'fail'],
    };
    const LEVEL_STYLE = {
        identical: 'ok', explained_not_identical: 'warn',
        investigate: 'bad', material_unexplained: 'bad',
    };
    const LEVEL_LABEL = {
        identical: 'IDENTICAL',
        explained_not_identical: 'NOT IDENTICAL — DIFFERENCES EXPLAINED',
        investigate: 'NOT IDENTICAL — UNEXPLAINED RESIDUAL',
        material_unexplained: 'MATERIAL DIFFERENCE',
    };

    function renderVerdict(r) {
        const a = r.assessment;
        const i = r.integrity;
        if (!a || !a.available) {
            const maiaErr = ((r.meta || {}).maia || {}).error;
            $('verdict').innerHTML = i ? `<div class="banner ${i.material ? 'bad' : 'warn'}">
                ${esc(i.verdict)}</div>`
                : maiaErr ? `<div class="banner warn"><b>ADMINISTRATOR-ONLY VIEW</b> —
                    ${esc(maiaErr)}. No front-office comparison for this date; the
                    tables below show the administrator's valuation alone.</div>` : '';
            return;
        }
        const ladder = (a.causes || []).map(c => {
            const [label, cls] = CAUSE_BADGE[c.status] || [c.status, ''];
            return `<tr><td class="lbl">${esc(c.cause)}
                <span class="badge ${cls}">${label}</span></td>
                ${diffCell(c.amount, c.status === 'definitional')}
                <td style="text-align:left;color:var(--text-muted);font-size:12px;
                           max-width:640px">${esc(c.action)}</td></tr>`;
        }).join('');
        const cp = a.constant_price;
        const chips = [];
        const cf = r.compliance_file || {};
        if (cf.report_date) {
            chips.push(cf.supplied_for_date
                ? `<span class="badge pass">compliance report supplied for ${esc(cf.report_date)}</span>`
                : `<span class="badge fail">compliance report NOT SUPPLIED for ${esc(cf.report_date)}`
                  + (cf.latest_on_file ? ` — latest on file ${esc(cf.latest_on_file)}` : '')
                  + `</span>`);
        }
        const fx = r.fx_forward_alert || {};
        if (fx.alert) {
            chips.push(`<span class="badge fail">share-class FX forwards distort `
                + fx.currencies.map(c => `${esc(c.currency)} ${c.pct_nav_fund}%→${c.pct_nav_all_in}%`).join(', ')
                + `</span>`);
        }
        $('verdict').innerHTML = `<div class="banner ${LEVEL_STYLE[a.level] || 'warn'}">
            <b>${LEVEL_LABEL[a.level] || a.level}</b> —
            front office ${num((i || {}).maia_total)} vs administrator ${num((i || {}).admin_nav)}
            = <b>${num(a.stated_difference)}</b> (${a.stated_pct > 0 ? '+' : ''}${a.stated_pct}%)
            <div style="font-size:13px;margin:6px 0 10px;opacity:.95">${esc(a.headline)}</div>
            <table style="margin-bottom:8px"><thead><tr><th>Cause</th><th>Amount</th>
                <th style="text-align:left">What it means / next action</th></tr></thead>
                <tbody>${ladder}</tbody></table>
            ${cp ? `<div style="font-size:12.5px;color:var(--text-secondary)">
                <b>Constant-price rerun</b> (${esc(cp.basis)}): pricing timing accounts for
                ${num(cp.pricing_timing_removed)}; the difference a single price source would
                NOT remove is <b>${num(cp.remaining_difference)}</b>
                (${cp.remaining_pct > 0 ? '+' : ''}${cp.remaining_pct}% of NAV).</div>` : ''}
            ${chips.length ? `<div style="margin-top:8px;display:flex;gap:8px;flex-wrap:wrap">
                ${chips.join('')}</div>` : ''}
        </div>`;
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

    // Main fund AUM: administrator NAV less the share-class overlay, with the
    // per-bond accrued restated to GA10's C+1 calc. The bridge is shown line
    // by line so the reader can see WHERE the restatement moves the number,
    // and the per-bond diffs are sorted biggest-first because a handful of
    // bonds carry nearly all of it.
    function renderMainFund(r) {
        const m = r.main_fund;
        if (!m || m.error || m.main_fund_aum == null) {
            $('mainFund').innerHTML = '<div class="muted">'
                + esc((m && m.error) || 'main fund breakdown unavailable')
                + '</div>';
            return;
        }
        const a = m.accrued || {};
        const bridge = [
            ['Administrator NAV', m.waystone_total_nav, ''],
            ['less share-class hedge P&amp;L', m.share_class_hedge_pnl,
             'overlay belongs to the hedged classes, not the fund'],
            ['accrued restated to GA10 C+1', a.delta_vs_waystone,
             'per-bond only — declared-but-unpaid income carried through'],
            ['<b>MAIN FUND AUM</b>', m.main_fund_aum, ''],
        ].map(([lbl, v, note], i, arr) => `<tr${i === arr.length - 1
                ? ' style="border-top:2px solid var(--line)"' : ''}>
                <td class="lbl">${lbl}${note
                    ? `<span class="muted"> — ${note}</span>` : ''}</td>
                <td style="text-align:right">${num(v)}</td></tr>`).join('');

        const comp = (m.breakdown || []).map(b => `<tr>
                <td class="lbl">${esc(b.label)}</td>
                <td style="text-align:right">${num(b.value)}</td>
                <td style="text-align:right">${b.pct_of_main_fund == null
                    ? '' : num(b.pct_of_main_fund, 2) + '%'}</td></tr>`).join('');

        // Only bonds GA10 actually restated carry a meaningful diff; the
        // retained ones are shown too, flagged, so a partial restatement
        // cannot read as a full one.
        const diffs = (a.rows || []).filter(x => x.diff_base).slice(0, 12)
            .map(x => `<tr><td class="lbl">${esc(x.isin)}
                    <span class="muted">${esc((x.description || '').slice(0, 34))}</span></td>
                <td>${num(x.waystone_accrued_base)}</td>
                <td>${num(x.ga10_accrued_base)}</td>
                ${diffCell(x.diff_base)}</tr>`).join('');

        const c = m.cash_check || {};
        $('mainFund').innerHTML =
            `<div class="src" style="margin-bottom:8px">${esc(m.basis)}</div>
             <div class="grid2">
               <div><table>${bridge}</table></div>
               <div><table><thead><tr><th>Component</th><th style="text-align:right">Value</th>
                    <th style="text-align:right">% of main fund</th></tr></thead>
                    <tbody>${comp}</tbody>
                    <tfoot><tr><td class="lbl"><b>Total</b></td><td></td>
                      <td style="text-align:right"><b>${num(m.breakdown_pct_total, 2)}%</b></td>
                    </tr></tfoot></table></div>
             </div>
             <div class="src" style="margin-top:10px">Accrued: administrator
                balance sheet ${num(a.waystone_balance_sheet)} = per-bond
                ${num(a.waystone_per_bond_sum)} + declared-but-unpaid
                ${num(a.declared_unpaid_income)}. GA10 C+1 restates the
                per-bond part to ${num(a.ga10_c1_per_bond)}
                (coverage ${esc(a.coverage)}${a.bonds_retained_at_waystone
                    ? `, ${a.bonds_retained_at_waystone} retained at the
                       administrator worth ${num(a.value_retained_at_waystone)}`
                    : ''}).</div>
             ${diffs ? `<h3 style="margin:12px 0 6px">Largest accrued differences</h3>
             <div class="scroll"><table><thead><tr><th>Bond</th><th>Waystone</th>
                <th>GA10 C+1</th><th>&Delta;</th></tr></thead>
                <tbody>${diffs}</tbody></table></div>` : ''}
             <div class="src" style="margin-top:10px"><b>Cash</b> — Maia
                ${num(c.maia)} vs administrator ${num(c.waystone)},
                difference ${num(c.difference)}${c.difference_pct_of_cash != null
                    ? ` (${num(c.difference_pct_of_cash, 2)}% of cash)` : ''}.
                ${esc(c.note || '')}</div>
             ${(m.caveats || []).map(x =>
                `<div class="src" style="margin-top:6px">! ${esc(x)}</div>`).join('')}`;
    }

    function renderAccrued(r) {
        const ar = r.accrued_recon;
        if (!ar || !(ar.rows || []).length) {
            $('accrued').innerHTML = '<div class="muted">no accrued data</div>';
            return;
        }
        const rows = ar.rows.map(x => {
            const dmw = x.diff_maia_waystone;
            const dgc = x.diff_ga10c1_waystone;
            return `<tr><td class="lbl">${esc(x.isin)}
                    <span class="muted">${esc(x.currency)}${x.coupon != null
                        ? ' ' + num(x.coupon, 3) + '%' : ''}</span></td>
                <td>${num(x.waystone_per100, 4)}</td>
                <td>${num(x.maia_per100, 4)}</td>
                <td>${num(x.ga10_t0_per100, 4)}</td>
                <td>${num(x.ga10_c1_per100, 4)}</td>
                ${diffCell(dmw)}
                ${diffCell(dgc)}</tr>`;
        }).join('');
        $('accrued').innerHTML =
            `<div class="src" style="margin-bottom:8px">${esc(ar.basis)}</div>
             <table><thead><tr><th>Bond</th><th>Waystone</th><th>Maia</th>
                <th>GA10 T+0</th><th>GA10 C+1</th>
                <th>&Delta; M&minus;W</th><th>&Delta; GA10c1&minus;W</th></tr></thead>
                <tbody>${rows}</tbody></table>
             <div class="src" style="margin-top:6px">GA10 coverage ${esc(ar.ga10_coverage)}`
            + ((ar.ga10_missing || []).length
                ? ` — not enrolled: ${ar.ga10_missing.map(esc).join(', ')}`
                : '') + `</div>`;
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
        const fe = c.fund_exposure || {};
        const base = fe.base_currency;
        const fx = r.fx_forward_alert || {};
        const flaggedCcy = new Set((fx.currencies || []).map(x => x.currency));
        const exp = (fe.rows || []).map(x => {
            const hot = flaggedCcy.has(x.currency);
            return `<tr${hot ? ' style="background:rgba(255,107,107,.06)"' : ''}>
                <td class="lbl">${esc(x.currency)}${x.currency === base
                    ? ' <span class="badge">base</span>' : ''}</td>
                <td>${num(x.bonds_base)}</td>
                <td>${num(x.fund_forward_base)}</td>
                <td>${num(x.share_class_forward_base)}</td>
                <td>${num(x.net_fund_base)}</td>
                <td>${x.pct_nav_fund == null ? DASH : num(x.pct_nav_fund, 2) + '%'}</td>
                <td class="${hot ? 'num-neg' : ''}">${x.pct_nav_all == null ? DASH
                    : num(x.pct_nav_all, 2) + '%'}</td></tr>`;
        }).join('');
        const sc = c.share_class_hedges || {};
        const cls = (sc.rows || []).map(x => `<tr>
            <td class="lbl">${esc(x.share_class)}</td>
            <td>${esc(x.currency)}</td>
            <td>${num(x.net_assets_local, 0)}</td>
            <td>${num(x.hedge_notional_local, 0)}</td>
            <td>${x.pct_hedged == null ? DASH : num(x.pct_hedged, 2) + '%'}</td>
            <td>${x.within_tolerance
                ? '<span class="badge pass">ok</span>'
                : '<span class="badge fail">outlier</span>'}</td></tr>`).join('');
        $('currency').innerHTML =
            (fx.alert ? `<div class="note err">&#9888; ${esc(fx.message)}</div>` : '')
            + (exp ? `<table style="margin-bottom:12px"><thead><tr><th>Ccy</th>
                <th>Bonds</th><th>Fund fwd</th><th>Share-class fwd</th>
                <th>Net (fund)</th><th>%NAV fund</th><th>%NAV all-in</th></tr></thead>
                <tbody>${exp}</tbody></table>` : '')
            + (cls ? `<table><thead><tr><th>Share class</th><th>Ccy</th>
                <th>Net assets (local)</th><th>Hedge notional</th><th>Hedged</th>
                <th></th></tr></thead><tbody>${cls}</tbody></table>` : '')
            + (!exp && !cls ? '<div class="muted">no currency data</div>' : '');
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
        // The ADMINISTRATOR's calendar drives the picker: Waystone strikes
        // no NAV on Irish holidays, and a Maia-led default used to pair
        // fresh Maia files against older packs. Latest Waystone first;
        // days without a same-day Maia view are labelled, not hidden.
        const d = await jget(`/aum/${state.fund}/dates`);
        const matched = new Set(d.matched_dates || []);
        const opts = (d.admin_dates || []).map(x =>
            `<option value="${x}">${x}${matched.has(x)
                ? '' : ' (admin only — no Maia view)'}</option>`);
        $('datePair').innerHTML = opts.join('') || '<option value="">no administrator packs</option>';
        state.date = $('datePair').value || null;
    }

    // Monotonic load sequence: a slow earlier request must never overwrite a
    // newer selection (the unpinned initial load once outlived a date-pinned
    // one and silently rendered the wrong pair — worst kind of wrong for a
    // recon page, where the numbers all LOOK plausible).
    let _loadSeq = 0;

    async function load() {
        const seq = ++_loadSeq;
        $('status').textContent = 'loading…';
        try {
            const url = `/aum/${state.fund}` + (state.date ? `?date=${state.date}` : '');
            const r = await jget(url);
            if (seq !== _loadSeq) return; // superseded by a newer selection
            renderBriefing(r);
            renderVerdict(r); renderDateWarning(r); renderAum(r); renderEvidence(r);
            renderAttribution(r); renderBonds(r); renderPrices(r); renderAccrued(r);
            renderMainFund(r);
            renderFx(r); renderCurrency(r);
            $('status').textContent = '';
            renderUploadHistory();
        } catch (e) {
            if (seq !== _loadSeq) return;
            $('status').innerHTML = `<span class="err">${esc(e.message)}</span>`;
        }
    }

    $('toggleDetail').addEventListener('click', () => {
        const d = $('detail');
        const open = d.style.display !== 'none';
        d.style.display = open ? 'none' : '';
        $('toggleDetail').innerHTML = open
            ? 'Show full detail &#9662;' : 'Hide full detail &#9652;';
    });

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
