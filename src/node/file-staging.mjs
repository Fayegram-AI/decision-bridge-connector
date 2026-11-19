import { randomUUID } from 'node:crypto';
import { dirname, resolve } from 'node:path';

/** A short sibling keeps atomic commits on the target filesystem without expanding its basename. */
export function stagingPath(target) {
    return resolve(dirname(target), `.dbc-${randomUUID()}.tmp`);
}
