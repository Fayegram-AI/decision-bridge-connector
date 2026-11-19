import { abortError, assert, bounded, delay, fingerprint, id, validateSignal } from '../util.mjs';
import { bindInput, shouldQuarantine, SOURCE_BINDING, sourceBinding } from '../node/source-recovery.mjs';
import { incomingMetadata } from '../source-binding.mjs';
import { createRedisSourceDrain } from './redis-source-drain.mjs';

/** Redis pending entries are acknowledged only after durable local admission. */
export function createRedisSource({ name = 'redis', revision = '1', url, client: supplied, stream, group, consumer = id('consumer'), field = 'payload', flow,
    inputMode = 'submission', pollMs = 100, claimIdleMs = 60000, quarantine = `${stream}:quarantine` } = {}) {
    const metadata = incomingMetadata(name, revision, 'explicit', true);
    for (const value of [stream, group, consumer, field, quarantine])
        assert(typeof value === 'string' && value.length > 0, 'Redis source names must be nonempty strings.');
    assert(Number.isSafeInteger(pollMs) && pollMs > 0 && pollMs <= 2147483647, 'Invalid Redis pollMs.');
    assert(Number.isSafeInteger(claimIdleMs) && claimIdleMs >= 0, 'Invalid Redis claimIdleMs.');
    assert(supplied === undefined || typeof supplied?.sendCommand === 'function', 'Redis source client requires sendCommand().');
    assert(quarantine !== stream, 'Redis quarantine must differ from the input stream.');
    const binding = sourceBinding({ flow, inputMode }, true);
    const stop = new AbortController();
    let client = supplied; let closed = false; let opened = false;
    const command = (args, signal) => bounded(child => client.sendCommand(args, { abortSignal: child }), { signal, timeoutMs: 10000 });
    const drain = createRedisSourceDrain({ owned: !supplied, stop() { closed = true; stop.abort(); },
        destroy() { if (client?.isOpen) client.destroy(); } });
    return {
        ...metadata,
        [SOURCE_BINDING]: binding,
        async *open({ signal } = {}) {
            validateSignal(signal);
            assert(!closed, 'Redis source is closed.', 'SOURCE_CLOSED');
            assert(!opened, 'Redis source already has a consumer.', 'SOURCE_BUSY');
            const lifetime = signal ? AbortSignal.any([signal, stop.signal]) : stop.signal;
            if (lifetime.aborted) throw abortError('redis-source');
            opened = true;
            if (!client) {
                const { createClient } = await import('redis');
                if (lifetime.aborted) throw abortError('redis-source');
                client = createClient({ url }); client.on('error', () => {});
                try { await bounded(() => client.connect(), { signal: lifetime, timeoutMs: 10000 }); }
                catch (error) { try { client.destroy(); } catch {} throw error; }
            }
            try { await command(['XGROUP', 'CREATE', stream, group, '0', 'MKSTREAM'], lifetime); }
            catch (error) { if (!String(error.message).startsWith('BUSYGROUP')) throw error; }
            let cursor = '0-0'; let lastClaim = 0;
            while (!lifetime.aborted) {
                let entries = [];
                if (Date.now() - lastClaim > 1000) {
                    const claimed = await command(['XAUTOCLAIM', stream, group, consumer, String(claimIdleMs), cursor, 'COUNT', '32'], lifetime);
                    cursor = claimed[0]; entries = claimed[1]; lastClaim = Date.now();
                }
                if (!entries.length) {
                    const reply = await command(['XREADGROUP', 'GROUP', group, consumer, 'COUNT', '32', 'STREAMS', stream, '>'], lifetime);
                    entries = reply instanceof Map ? reply.get(stream) ?? [] : Array.isArray(reply) ? reply[0]?.[1] ?? [] : reply?.[stream] ?? [];
                }
                for (const [entryId, pairs] of entries) {
                    if (lifetime.aborted) return;
                    // Yielded messages retain independent settlement budgets during ingress drain.
                    const reject = async (error, signal) => {
                        if (!shouldQuarantine(error)) return;
                        await command(['XADD', quarantine, 'MAXLEN', '~', '1000', '*', 'source_id', entryId, 'error', JSON.stringify(error)], signal);
                        await command(['XACK', stream, group, entryId], signal);
                    };
                    let input;
                    try {
                        const values = Object.fromEntries(Array.from({ length: pairs.length / 2 }, (_, index) => [pairs[index * 2], pairs[index * 2 + 1]]));
                        assert(typeof values[field] === 'string' && Buffer.byteLength(values[field]) <= 1048576, 'Invalid Redis payload.', 'INVALID_JSON');
                        input = bindInput(JSON.parse(values[field]), { flow, inputMode }, await fingerprint([stream, group, entryId]));
                    } catch {
                        if (lifetime.aborted) return;
                        await reject({ code: 'INVALID_JSON', message: 'Invalid stream entry.' }, lifetime); continue;
                    }
                    if (lifetime.aborted) return;
                    yield { input, ...drain.track({ acknowledge: async () => {
                        const acknowledged = await command(['XACK', stream, group, entryId], drain.signal);
                        assert(Number.isSafeInteger(acknowledged) && acknowledged >= 0, 'Invalid Redis acknowledgement.', 'INVALID_RESPONSE');
                        return { status: 'acknowledged', receipt: { stream, group, entryId, acknowledged } };
                    },
                        reject: error => reject(error, drain.signal) }) };
                }
                if (!entries.length) await delay(pollMs, lifetime);
            }
        },
        close: options => drain.close(options),
    };
}
