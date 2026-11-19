import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';

/** Invoke npm's JavaScript entry on Windows without interpolating paths through a shell. */
export function npmCommand(args) {
    if (process.platform !== 'win32') return { command: 'npm', args };
    const cli = process.env.npm_execpath ?? join(dirname(process.execPath), 'node_modules', 'npm', 'bin', 'npm-cli.js');
    if (!existsSync(cli) || !/\.[cm]?js$/i.test(cli)) throw new Error('Cannot locate npm CLI; invoke this check through npm run.');
    return { command: process.execPath, args: [cli, ...args] };
}
