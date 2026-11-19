import { assert, snapshot } from '../util.mjs';
import { restoreReferences } from './data-ref.mjs';

export const TABLES = ['tasks', 'jobs', 'dedup', 'controls', 'aggregates', 'blobs', 'chunks', 'refs', 'owners', 'records', 'frames'];
export const COLUMNS = ['id', 'namespace', 'flow', 'kind', 'state', 'taskId', 'destination', 'placement',
    'required', 'runAt', 'leaseUntil', 'createdAt', 'expiresAt', 'token', 'priority', 'sequence', 'orderingKey', 'rootId', 'revision', 'ruleId',
    'ownerId', 'blobId', 'ordinal', 'bytes'];
export const NUMERIC = new Set(['required', 'runAt', 'leaseUntil', 'createdAt', 'expiresAt', 'priority', 'sequence', 'ordinal', 'bytes']);

export function tableName(name) {
    assert(TABLES.includes(name), 'Unknown storage collection.');
    return `dbc3_${name}`;
}

export function querySql(where, dialect, start = 0, after) {
    const parameters = [];
    const bind = value => { parameters.push(value); return dialect === 'pg' ? `$${start + parameters.length}` : '?'; };
    const clauses = Object.entries(where).map(([column, value]) => {
        assert(COLUMNS.includes(column), 'Unknown query column.');
        if (value === null) return `${column} IS NULL`;
        if (typeof value !== 'object') return `${column} = ${bind(value)}`;
        return Object.entries(value).map(([operator, operand]) => {
            const operators = { lt: '<', lte: '<=', gt: '>', gte: '>=', ne: '<>' };
            if (operator === 'in') {
                assert(Array.isArray(operand) && operand.length > 0 && operand.length <= 256, 'Invalid query set.');
                return `${column} IN (${operand.map(bind).join(',')})`;
            }
            assert(operators[operator], 'Unknown query operator.');
            return `${column} ${operators[operator]} ${bind(operand)}`;
        }).join(' AND ');
    });
    if (after) {
        assert(after.columns.length > 0 && after.columns.length === after.values.length &&
            after.columns.every(column => COLUMNS.includes(column)), 'Invalid query cursor.');
        clauses.push(`(${after.columns.join(',')}) > (${after.values.map(bind).join(',')})`);
    }
    return { text: clauses.length ? clauses.join(' AND ') : '1=1', parameters };
}

export function compareRows(a, b, order) {
    for (const key of order) if (a[key] !== b[key]) return a[key] < b[key] ? -1 : 1;
    return 0;
}

export function matches(row, where) {
    return Object.entries(where).every(([key, value]) => {
        if (value === null) return row[key] == null;
        if (typeof value !== 'object') return row[key] === value;
        if (row[key] == null) return false;
        return Object.entries(value).every(([operator, operand]) => ({
            lt: () => row[key] < operand, lte: () => row[key] <= operand, gt: () => row[key] > operand,
            gte: () => row[key] >= operand, ne: () => row[key] !== operand, in: () => operand.includes(row[key]),
        })[operator]());
    });
}

export function rowValues(row) {
    const { data, ...metadata } = row;
    return [...COLUMNS.map(key => key === 'ruleId' ? (typeof row.ruleId === 'string' && row.ruleId.length ? row.ruleId : null) : row[key] ?? null),
        JSON.stringify(snapshot(metadata, { maxBytes: 65536 })), data ?? null];
}

export function readRow(row) {
    if (!row) return null;
    return { ...restoreReferences(JSON.parse(row.value)), ...(row.data == null ? {} : { data: new Uint8Array(row.data) }) };
}

export function schemaSql(dialect) {
    return TABLES.flatMap(name => {
        const table = tableName(name);
        return [`CREATE TABLE IF NOT EXISTS ${table} (${COLUMNS.map(key => `${key} ${NUMERIC.has(key) ? 'BIGINT' : 'TEXT'}${key === 'id' ? ' PRIMARY KEY' : ''}`).join(',')}, value TEXT NOT NULL, data ${dialect === 'pg' ? 'BYTEA' : 'BLOB'})`,
            ...[
                ['namespace', 'kind', 'state', 'runAt', 'priority', 'id'], ['namespace', 'taskId', 'id'],
                ['namespace', 'flow', 'state', 'id'], ['namespace', 'destination', 'state', 'leaseUntil'],
                ['namespace', 'state', 'leaseUntil'], ['namespace', 'expiresAt', 'id'], ['namespace', 'createdAt', 'id'],
                ['namespace', 'orderingKey', 'sequence', 'state'],
                ['namespace', 'rootId', 'id'],
                ['namespace', 'revision', 'kind', 'state', 'runAt'],
                ['namespace', 'id'],
                ['namespace', 'ownerId', 'ordinal', 'id'], ['namespace', 'blobId', 'ordinal', 'id'],
                ['namespace', 'kind', 'rootId', 'state'],
            ].map((columns, index) => `CREATE INDEX IF NOT EXISTS ${table}_i${index} ON ${table} (${columns.join(',')})`),
            ...(['refs', 'records', 'frames'].includes(name) ? [`CREATE INDEX IF NOT EXISTS ${table}_owner ON ${table} (ownerId,id)`] : []),
            ...(name === 'refs' ? [`CREATE INDEX IF NOT EXISTS ${table}_blob ON ${table} (blobId,id)`] : []),
            ...(name === 'aggregates' ? [
                `CREATE INDEX IF NOT EXISTS ${table}_rule ON ${table} (namespace,ruleId)`,
                `CREATE INDEX IF NOT EXISTS ${table}_ready ON ${table} (namespace,kind,id) WHERE required=1 AND state IN ('complete','incomplete')`,
            ] : [])];
    });
}

/** Bound every page; cursors use stable identifiers rather than OFFSET scans. */
export function orderSql(order = ['id']) {
    assert(order.every(column => COLUMNS.includes(column)), 'Invalid query order.');
    return order.join(',');
}
