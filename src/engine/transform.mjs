import { assert, freeze, getPath, isRecord, keysOnly, snapshot } from '../util.mjs';
import { project, validatePath, validateProjection } from '../mapping.mjs';

/** Text templates escape interpolated data; they never execute expressions. */
function escape(value, mode) {
    const text = typeof value === 'string' ? value : JSON.stringify(value);
    if (mode === 'json') return JSON.stringify(text).slice(1, -1);
    if (mode === 'html') return text.replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
    if (mode === 'uri') return encodeURIComponent(text);
    return text;
}

export function validateTransform(steps = []) {
    assert(Array.isArray(steps) && steps.length <= 32, 'Use at most 32 transformation steps.');
    for (const step of steps) {
        assert(isRecord(step), 'Invalid transformation.');
        switch (step.type) {
            case 'project': keysOnly(step, ['type', 'value']); validateProjection(step.value); break;
            case 'json': keysOnly(step, ['type']); break;
            case 'text':
                keysOnly(step, ['type', 'template', 'escape']);
                assert(typeof step.template === 'string' && step.template.length <= 65536, 'Invalid text template.');
                assert(['json', 'html', 'uri', 'none'].includes(step.escape ?? 'json'), 'Invalid escape mode.');
                for (const match of step.template.matchAll(/\{\{([^{}]+)\}\}/g)) validatePath(match[1].trim());
                break;
            case 'coerce':
                keysOnly(step, ['type', 'to']);
                assert(['string', 'number', 'boolean'].includes(step.to), 'Invalid coercion.');
                break;
            case 'module': keysOnly(step, ['type', 'name', 'options']); assert(typeof step.name === 'string', 'Transform name required.'); break;
            default: assert(false, 'Unknown transformation.');
        }
    }
    return freeze(snapshot(steps));
}

export async function transform(value, steps = [], modules = {}, { maxBytes = 1048576 } = {}) {
    let result = snapshot(value, { maxBytes });
    for (const step of validateTransform(steps)) {
        if (step.type === 'project') result = project(result, step.value);
        if (step.type === 'json') result = JSON.stringify(result);
        if (step.type === 'text') result = step.template.replace(/\{\{([^{}]+)\}\}/g, (_match, path) => {
            const selected = getPath(result, path.trim());
            assert(selected !== undefined, `Missing template value: ${path}`, 'MAPPING_MISSING_PATH');
            return escape(selected, step.escape ?? 'json');
        });
        if (step.type === 'coerce') {
            if (step.to === 'string') {
                assert(result === null || ['string', 'number', 'boolean'].includes(typeof result), 'Only scalar values can become strings.');
                result = String(result);
            } else if (step.to === 'number') {
                assert(typeof result === 'number' || typeof result === 'string' && result.trim() !== '', 'Invalid number coercion.');
                result = Number(result);
                assert(Number.isFinite(result), 'Invalid number coercion.');
            } else {
                assert([true, false, 'true', 'false'].includes(result), 'Boolean coercion accepts true or false.');
                result = result === true || result === 'true';
            }
        }
        if (step.type === 'module') {
            assert(typeof modules[step.name]?.execute === 'function', `Missing transform: ${step.name}`);
            result = await modules[step.name].execute(freeze(snapshot(result)), step.options ?? {});
        }
        result = snapshot(result, { maxBytes });
    }
    return freeze(result);
}
