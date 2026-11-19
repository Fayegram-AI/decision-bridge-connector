import { constants } from 'node:fs';
import { link, lstat, mkdir, open, readdir, rename, rmdir, stat, unlink, utimes } from 'node:fs/promises';
import { basename, join, resolve } from 'node:path';
import { abortError, assert, ConnectorError, delay, fingerprintCaptured, id, validateSignal } from '../util.mjs';
import { incomingMetadata } from '../source-binding.mjs';
import { createSourceSettlements } from './source-settlements.mjs';
import { readJson } from './io.mjs';
import { bindInput, captureIncomingValue, shouldQuarantine, SOURCE_BINDING, sourceBinding } from './source-recovery.mjs';

/** Enumeration is only a hint: producers can replace a queued entry before its claim. */
async function readClaim(path, signal) {
    const expected = await lstat(path, { bigint: true });
    assert(expected.isFile(), 'Spool input must be a regular file.', 'INVALID_SOURCE_MESSAGE');
    // No-follow protects POSIX opens; nonblocking prevents a raced FIFO from stalling open().
    const handle = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0) | (constants.O_NONBLOCK ?? 0))
        .catch(error => {
            if (error.code === 'ELOOP') throw new ConnectorError('INVALID_SOURCE_MESSAGE', 'Spool input must be a regular file.');
            throw error;
        });
    let input;
    try {
        const actual = await handle.stat({ bigint: true });
        // BigInts preserve Windows file IDs beyond Number's safe integer range.
        assert(actual.isFile() && actual.dev === expected.dev && actual.ino === expected.ino,
            'Spool input changed while being opened.', 'INVALID_SOURCE_MESSAGE');
        input = handle.createReadStream({ autoClose: false });
        return await readJson('-', { stdin: input, maxBytes: 1048576, signal });
    } finally { input?.destroy(); await handle.close(); }
}

/** Return a claim without ever replacing a newer producer file at the original name. */
async function requeue(path, target) {
    try { await link(path, target); }
    catch (error) { if (['EEXIST', 'ENOENT'].includes(error.code)) return; throw error; }
    await unlink(path).catch(error => { if (error.code !== 'ENOENT') throw error; });
}

/** Never remove evidence when a producer or recovery worker still owns a file. */
async function removeEmpty(directory) {
    await rmdir(directory).catch(error => { if (!['ENOENT', 'ENOTEMPTY', 'EEXIST'].includes(error.code)) throw error; });
}

/** New claims preserve full filenames; old flat claims remain recoverable after upgrade. */
async function recover(entry, working, root) {
    const container = join(working, entry.name);
    if (entry.isDirectory()) {
        if (!/^claim_[0-9a-f-]+$/.test(entry.name)) return;
        const files = await readdir(container, { withFileTypes: true }).catch(error => {
            if (error.code === 'ENOENT') return null;
            throw error;
        });
        if (!files) return;
        if (files.length === 1 && files[0].isFile()) await requeue(join(container, files[0].name), join(root, files[0].name));
        await removeEmpty(container);
    } else {
        let original;
        try { original = decodeURIComponent(entry.name.split('~')[0]); } catch { return; }
        if (basename(original) === original) await requeue(container, join(root, original));
    }
}

/** Atomic claims and stable input identity support at-least-once local admission. */
export function createSpoolSource({ name = 'spool', revision = '1', directory, flow, inputMode = 'submission', pollMs = 100, leaseMs = 60000 } = {}) {
    if (process.platform === 'win32') {
        const [major, minor] = process.versions.uv.split('.').map(Number);
        // Older Windows fast stat APIs can report device 0 for a real file while
        // handle.stat() reports its volume. Refuse before claiming any evidence.
        assert(major > 1 || major === 1 && minor >= 51,
            'Windows spool sources require libuv 1.51 or later. Use Node 22.17 or later in Node 22, or Node 24.2 or later.',
            'UNSUPPORTED_RUNTIME');
    }
    const binding = sourceBinding({ flow, inputMode }, true);
    const metadata = incomingMetadata(name, revision, 'explicit', true);
    for (const value of [pollMs, leaseMs]) assert(Number.isSafeInteger(value) && value > 0 && value <= 2147483647, 'Invalid spool time limit.');
    const root = resolve(directory); const working = join(root, '.processing');
    const archived = join(root, '.processed'); const quarantine = join(root, '.quarantine');
    const stop = new AbortController(); let opened = false;
    const settlements = createSourceSettlements(() => stop.abort());
    return { ...metadata, [SOURCE_BINDING]: binding, close: options => settlements.close(options), async *open({ signal } = {}) {
        validateSignal(signal);
        assert(!opened, 'Spool source already has a consumer.', 'SOURCE_BUSY');
        if (stop.signal.aborted || signal?.aborted) throw abortError('spool-source');
        signal = signal ? AbortSignal.any([signal, stop.signal]) : stop.signal; opened = true;
        for (const path of [root, working, archived, quarantine]) await mkdir(path, { recursive: true });
        let after; let queuedAfter;
        while (!signal.aborted) {
            const entries = (await readdir(working, { withFileTypes: true })).filter(entry => entry.isFile() || entry.isDirectory())
                .sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
            let page = entries.filter(entry => after === undefined || entry.name > after).slice(0, 100);
            if (!page.length && after !== undefined) page = entries.slice(0, 100);
            after = page.at(-1)?.name;
            for (const entry of page) {
                const path = join(working, entry.name);
                const info = await stat(path).catch(() => null);
                if (!info || Date.now() - info.mtimeMs < leaseMs) continue;
                await recover(entry, working, root);
            }
            const queued = (await readdir(root, { withFileTypes: true }))
                .filter(entry => entry.isFile() && entry.name.endsWith('.json'))
                .sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
            let files = queued.filter(entry => queuedAfter === undefined || entry.name > queuedAfter).slice(0, 100);
            if (!files.length && queuedAfter !== undefined) files = queued.slice(0, 100);
            // Retryable files remain queued but cannot monopolize the first bounded page.
            queuedAfter = files.at(-1)?.name;
            for (const entry of files) {
                if (signal.aborted) break;
                // Appending identity to a valid filename can exceed the filesystem component limit.
                const claim = id('claim'); const container = join(working, claim);
                const path = join(container, entry.name);
                await mkdir(container, { mode: 0o700 });
                try { await rename(join(root, entry.name), path); }
                catch (error) { await removeEmpty(container); if (error.code === 'ENOENT') continue; throw error; }
                await utimes(container, new Date(), new Date());
                const heartbeat = setInterval(() => { void utimes(container, new Date(), new Date()).catch(() => {}); }, Math.max(1, Math.floor(leaseMs / 3)));
                heartbeat.unref?.(); let yielded = false;
                try {
                    let value;
                    try { value = captureIncomingValue(await readClaim(path, signal), { inputMode }); }
                    catch (error) {
                        // Validate the same JSON contract used by fingerprinting, independently of wire bytes.
                        if (error instanceof ConnectorError && ['VALIDATION_ERROR', 'RESOURCE_LIMIT'].includes(error.code))
                            throw new ConnectorError(error.code === 'RESOURCE_LIMIT' ? 'INPUT_TOO_LARGE' : 'INVALID_JSON',
                                'Spool input exceeds the supported JSON contract.');
                        throw error;
                    }
                    const key = await fingerprintCaptured([entry.name, value]);
                    if (signal.aborted) throw abortError('spool-source');
                    const input = bindInput(value, { flow, inputMode }, key);
                    const callbacks = settlements.track({
                        acknowledge: () => rename(container, join(archived, claim)),
                        reject: async error => {
                            if (shouldQuarantine(error)) await rename(container, join(quarantine, claim));
                            else { await requeue(path, join(root, entry.name)); await removeEmpty(container); }
                        } }, () => clearInterval(heartbeat));
                    yielded = true;
                    yield { input, ...callbacks };
                } catch (error) {
                    if (shouldQuarantine(error)) await rename(container, join(quarantine, claim));
                    else if (!signal.aborted) throw error;
                    // Interrupted reads remain claimed until recovery; they are not malformed input.
                } finally { if (!yielded) clearInterval(heartbeat); }
            }
            await delay(pollMs, signal);
        }
    } };
}
