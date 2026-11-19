import { createServer } from 'node:net';
import { once } from 'node:events';

const encode = value => Array.isArray(value) ? `*${value.length}\r\n${value.map(encode).join('')}`
    : typeof value === 'number' ? `:${value}\r\n` : `$${Buffer.byteLength(value)}\r\n${value}\r\n`;

/** A small RESP peer exercises the installed Redis client's real socket lifecycle. */
export async function redisPeer(handle) {
    const commands = []; const sockets = new Set();
    const server = createServer(socket => {
        sockets.add(socket); socket.on('close', () => sockets.delete(socket)); socket.on('error', () => {});
        let pending = Buffer.alloc(0);
        socket.on('data', chunk => {
            pending = Buffer.concat([pending, chunk]);
            while (pending.length) {
                const first = pending.indexOf('\r\n'); if (first < 0) return;
                const count = Number(pending.subarray(1, first)); const args = []; let cursor = first + 2;
                for (let index = 0; index < count; index++) {
                    const end = pending.indexOf('\r\n', cursor); if (end < 0) return;
                    const length = Number(pending.subarray(cursor + 1, end)); const start = end + 2;
                    if (pending.length < start + length + 2) return;
                    args.push(pending.subarray(start, start + length).toString('utf8')); cursor = start + length + 2;
                }
                pending = pending.subarray(cursor); commands.push(args);
                const fallback = args[0] === 'XAUTOCLAIM' ? ['0-0', []] : args[0] === 'XREADGROUP'
                    ? [['events', [['1-0', ['payload', '42']]]]] : args[0] === 'XACK' ? 1 : 'OK';
                void Promise.resolve().then(() => handle?.(args) ?? fallback).then(value => {
                    if (!socket.destroyed) socket.write(encode(value));
                }, () => socket.destroy());
            }
        });
    });
    server.listen(0, '127.0.0.1'); await once(server, 'listening');
    return { url: `redis://127.0.0.1:${server.address().port}`, commands, sockets,
        async close() { for (const socket of sockets) socket.destroy(); await new Promise(resolve => server.close(resolve)); } };
}
