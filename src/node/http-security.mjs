import { lookup } from 'node:dns/promises';
import { isIP } from 'node:net';
import { assert, ConnectorError } from '../util.mjs';

const loopback = ['localhost', '127.0.0.1', '[::1]'];

export function validateBearerToken(token) {
    assert(token === undefined || typeof token === 'string' && token.trim().length > 0,
        'authToken must be a nonempty string.');
}

export function validateListenerAuthentication({ host, authToken, authorize }) {
    validateBearerToken(authToken);
    assert(typeof host === 'string' && host.length > 0 && !/[\s\x00-\x1f\x7f/\\[\]]/.test(host) &&
        (!host.includes(':') || isIP(host) === 6), 'Invalid listener host.');
    assert(['127.0.0.1', '::1', 'localhost'].includes(host) || authToken !== undefined || typeof authorize === 'function',
        'Network listeners require authentication.');
}

export function validateListenerPort(port) {
    const value = typeof port === 'string' && port.trim() ? Number(port) : port;
    assert(Number.isInteger(value) && value >= 0 && value <= 65535, 'Invalid listener port.');
    return value;
}

export async function listenerAddress(host, port) {
    const numericPort = validateListenerPort(port);
    let bindHost = host;
    if (!isIP(host)) {
        const addresses = await lookup(host, { all: true }).catch(() => {
            throw new ConnectorError('CONFIGURATION_ERROR', 'Listener host could not be resolved.');
        });
        // Match the TCP server's preference for the first non-link-local result.
        bindHost = (addresses.find(item => !/^fe[89ab][\da-f]:/i.test(item.address)) ?? addresses[0])?.address;
        assert(bindHost, 'Listener host did not resolve to an address.');
    }
    const family = isIP(bindHost);
    const canonical = family === 6 && !bindHost.includes('%')
        ? new URL(`http://[${bindHost}]/`).hostname.slice(1, -1) : bindHost;
    return Object.freeze({ host: canonical, bindHost, port: numericPort });
}

/** Compare the same addresses that the listeners will bind. */
export function overlapsListener(left, right) {
    if (!left.port || left.port !== right.port) return false;
    const compareHost = ({ host }) => {
        const mapped = /^::ffff:([\da-f]+):([\da-f]+)$/.exec(host);
        if (!mapped) return host;
        const [high, low] = mapped.slice(1).map(word => Number.parseInt(word, 16));
        return [high >> 8, high & 255, low >> 8, low & 255].join('.');
    };
    const a = compareHost(left); const b = compareHost(right);
    if (a === b || a === '::' || b === '::') return true;
    return isIP(a) === 4 && isIP(b) === 4 && (a === '0.0.0.0' || b === '0.0.0.0');
}

/** Parse an HTTP authority without accepting credentials, paths or forwarded headers. */
function hostname(value) {
    if (typeof value !== 'string' || value.length > 1024 ||
        !/^(?:\[[0-9a-f:.]+\]|[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?)(?::\d{1,5})?$/i.test(value)) return null;
    try { return new URL(`http://${value}`).hostname; }
    catch { return null; }
}

/** Capture deployment authority once; loopback binding alone does not trust browser requests. */
export function createRequestPolicy({ allowedHosts = loopback, allowedOrigin } = {}) {
    assert(Array.isArray(allowedHosts) && allowedHosts.length > 0 && allowedHosts.length <= 128,
        'allowedHosts must contain 1–128 hostnames.');
    const hosts = new Set([...allowedHosts].map(value => {
        const host = hostname(value);
        assert(host && !value.replace(/^\[[^\]]+\]/, '').includes(':'),
            'allowedHosts entries must be hostnames or bracketed IPv6 addresses without ports.');
        return host;
    }));
    if (allowedOrigin !== undefined) {
        let origin;
        try { origin = new URL(allowedOrigin); } catch { /* Reject malformed configuration below. */ }
        assert(typeof allowedOrigin === 'string' && origin && ['http:', 'https:'].includes(origin.protocol) &&
            origin.origin === allowedOrigin, 'allowedOrigin must be an exact HTTP or HTTPS origin without a path.');
    }
    return request => {
        const host = hostname(request.headers.host);
        const origin = request.headers.origin;
        return hosts.has(host) && (origin === undefined || allowedOrigin !== undefined && origin === allowedOrigin);
    };
}
