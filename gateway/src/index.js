/**
 * recon.x-trillion.com → recon-mcp on Railway.
 *
 * Pure pass-through: path, query, method, headers (cookies included) and
 * body all forward unchanged. X-Forwarded-Host/Proto are set so recon-mcp's
 * login redirect builds return_to against THIS hostname, not the Railway one.
 *
 * The origin is pinned here deliberately — this worker IS the public name
 * for that origin (same as the other x-trillion gateway workers); auth-mcp
 * indirection would add a per-request lookup for a URL that changes only
 * when this file changes anyway.
 */
const ORIGIN = 'https://recon-mcp-production.up.railway.app';

export default {
    async fetch(request) {
        const url = new URL(request.url);
        const target = ORIGIN + url.pathname + url.search;
        const headers = new Headers(request.headers);
        // Railway's edge OVERWRITES X-Forwarded-Host with its own hostname
        // (verified 2026-08-02: return_to came back as the Railway URL), so
        // the original host also travels in a custom header it leaves alone.
        headers.set('X-Forwarded-Host', url.hostname);
        headers.set('X-Recon-Public-Host', url.hostname);
        headers.set('X-Forwarded-Proto', 'https');
        return fetch(target, {
            method: request.method,
            headers,
            body: ['GET', 'HEAD'].includes(request.method) ? undefined : request.body,
            redirect: 'manual',
        });
    },
};
