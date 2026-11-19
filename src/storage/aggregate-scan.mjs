/** Merge eligible windows and closed retries into one bounded, fair identifier page. */
export async function scanDueAggregates(tx, namespace, runtimeRevision) {
    const cursorId = JSON.stringify([namespace, `aggregate-cursor:${runtimeRevision ?? '*'}`]);
    await tx.lock(cursorId);
    const where = { namespace, ...(runtimeRevision ? { kind: runtimeRevision, required: 1 } : {}),
        state: { in: ['pending', 'complete', 'incomplete'] }, expiresAt: { lte: Date.now() } };
    const closed = { namespace, ...(runtimeRevision ? { kind: runtimeRevision } : {}), required: 1,
        state: { in: ['complete', 'incomplete'] } };
    const cursor = await tx.get('controls', cursorId);
    async function page(after) {
        // Older closed groups can still carry their original collection-window
        // expiry. They are ready to retry without changing that saved evidence.
        const query = { limit: 100, ...(after ? { after: { id: after } } : {}) };
        const [expired, completed] = await Promise.all([
            tx.find('aggregates', where, query), tx.find('aggregates', closed, query),
        ]);
        const candidates = [...new Set([...expired, ...completed].map(row => row.id))];
        // Cursor order must follow the repository's collation, including when it
        // differs from JavaScript ordering. Two pages contribute at most 200 IDs.
        return candidates.length ? tx.find('aggregates', { namespace, ...(runtimeRevision ? { kind: runtimeRevision, required: 1 } : {}),
            id: { in: candidates } }, { limit: 100 }) : [];
    }
    let rows = await page(cursor?.cursor);
    if (!rows.length && cursor) rows = await page();
    if (rows.length) await tx.put('controls', { id: cursorId, namespace, kind: 'aggregate-cursor',
        cursor: rows.at(-1).id, expiresAt: Date.now() + 600000 });
    else await tx.remove('controls', cursorId);
    return rows;
}
