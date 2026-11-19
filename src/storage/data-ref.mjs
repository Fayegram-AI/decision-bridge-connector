import { isRecord } from '../util.mjs';

// Only storage-produced metadata can bypass application-value encoding. A JSON
// property named $data in a submitted value remains ordinary application data.
const generated = new WeakSet();
export const isDataReference = value => value !== null && typeof value === 'object' && generated.has(value);
export function dataReference(value) { generated.add(value); return value; }
export const referenceNode = value => isRecord(value) && Object.hasOwn(value, '$data') && isRecord(value.$data) &&
    typeof value.$data.sha256 === 'string' && typeof value.$data.kind === 'string';
export function restoreReferences(metadata) {
    if (!metadata || typeof metadata !== 'object') return metadata;
    if (referenceNode(metadata)) { dataReference(metadata); return metadata; }
    Object.values(metadata).forEach(restoreReferences); return metadata;
}
