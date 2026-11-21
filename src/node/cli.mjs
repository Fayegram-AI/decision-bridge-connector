import { createReadStream } from 'node:fs';
import { loadConfig, createConfiguredRunner, inspectConfiguration } from './configuration.mjs';
import { createHost } from './host.mjs';
import { invoke } from './http-service.mjs';
import { bindInput } from './incoming.mjs';
import { jsonLines, readJson, writeStream } from './io.mjs';
import { assert, bounded, publicError } from '../util.mjs';
import { VERSION } from '../contracts.mjs';
import { cleanupDiagnostic, closeEvery } from './cleanup.mjs';

const HELP = `decision-bridge-connectors <command> --config FILE [options]

run | submit | preview          Read one submission from --input FILE (default stdin).
batch                           Read JSONL, preserving input order and per-line failures.
serve                           Run incoming connections and workers; --role all|ingress|worker.
listen NAME                     Run one configured incoming connection with workers.
config inspect                  Inspect flows without resolving credentials or opening integrations.
status ID | list | inspect       Read stored execution state.
retry DELIVERY | resolve DELIVERY | completion DELIVERY
resolve-evaluation ID            Read a recovered result or confirmed-failure resolution.
cancel ID | reprocess ID | redeliver ID | reevaluate ID
export ID | import | recover | maintain | pause | resume

--flow NAME --kind request|event|result   Wrap input in a flow submission.
--reply report|raw|final --policy NAME    Select caller output independently of deliveries.
--wait admitted|evaluated|required|settled --wait-ms NUMBER
--input FILE --options FILE              JSON input and operation options.

Stdout contains JSON or JSONL. Diagnostics use stderr. Exit codes: 0 success,
1 operational failure, 2 invalid input/configuration, 130 interrupted.
`;

function parse(argv) {
    const positional = []; const flags = {};
    for (let i = 0; i < argv.length; i++) {
        if (argv[i].startsWith('--')) {
            const key = argv[i].slice(2);
            assert(['config', 'input', 'flow', 'kind', 'reply', 'policy', 'wait', 'wait-ms', 'options', 'role'].includes(key), `Unknown flag: ${key}`);
            assert(argv[i + 1] && !argv[i + 1].startsWith('--'), `Flag requires a value: ${key}`); flags[key] = argv[++i];
        } else positional.push(argv[i]);
    }
    return { positional, flags };
}

/** The runner checks flow and transfer capacity before it asks for archive bytes. */
function importSource(path, stdin, signal) {
    if (!path || path === '-') return stdin;
    return { [Symbol.asyncIterator]: () => createReadStream(path, { signal })[Symbol.asyncIterator]() };
}

const invalidInputCodes = new Set(['CONFIGURATION_ERROR', 'VALIDATION_ERROR', 'INVALID_JSON', 'INPUT_TOO_LARGE',
    'INVALID_UTF8', 'INVALID_TRANSFER', 'UNSUPPORTED_SCHEMA', 'CREDENTIAL_UNAVAILABLE', 'NOT_FOUND', 'CONFLICT']);

/** Syntax/configuration validation differs from failures of a running integration. */
function failureExitCode(error, phase) {
    const failure = publicError(error);
    if (['arguments', 'configuration'].includes(phase) || failure.inputInvalid === true || invalidInputCodes.has(failure.code)) return 2;
    try { if (phase === 'startup' && (error instanceof TypeError || error instanceof SyntaxError)) return 2; }
    catch { /* An opaque thrown value remains an operational failure. */ }
    return 1;
}

async function readPart(runner, taskId, path) {
    const lease = await runner.openSnapshot(taskId);
    try {
        let after;
        do {
            const page = await runner.listSnapshot(lease.id, { collection: 'data', after });
            const part = page.items.find(item => item.entity === 'task' && JSON.stringify(item.path) === JSON.stringify(path));
            if (part) return await runner.readValue(lease.id, part.id);
            after = page.next;
        } while (after);
        return null;
    } finally { await runner.closeSnapshot(lease.id); }
}
async function output(value, flags, runner) {
    if (!flags.reply || flags.reply === 'report') return value;
    if (flags.reply === 'raw') return readPart(runner, value.taskId, ['result']);
    assert(flags.reply === 'final' && flags.policy, 'Final reply requires --policy.');
    return readPart(runner, value.taskId, ['policies', flags.policy, 'decision']);
}

/** Caller output selection must fail before admitting work or changing saved work. */
function validateReply(flags, command, definition) {
    if (!flags.reply || flags.reply === 'report') return;
    assert(['raw', 'final'].includes(flags.reply), 'Reply must be report, raw or final.');
    assert(['run', 'submit', 'batch', 'status', 'retry', 'resolve', 'resolve-evaluation', 'completion', 'cancel',
        'reprocess', 'redeliver', 'reevaluate', 'import'].includes(command), 'Reply selection requires an execution operation.');
    if (flags.reply !== 'final') return;
    assert(flags.policy, 'Final reply requires --policy.');
    if (definition !== undefined) assert(Object.hasOwn(definition.policies ?? {}, flags.policy), 'Reply policy is not defined by the selected flow.');
}

export async function main(argv, { stdin = process.stdin, stdout = process.stdout, stderr = process.stderr } = {}) {
    if (argv.includes('--version')) { await writeStream(stdout, `${VERSION}\n`); return 0; }
    if (!argv.length || argv.includes('--help')) { await writeStream(stdout, HELP); return 0; }
    let runner; let host; let interrupted = false; let phase = 'arguments';
    const controller = new AbortController();
    const stop = () => { interrupted = true; controller.abort(); };
    process.once('SIGINT', stop); process.once('SIGTERM', stop);
    const operation = async () => { try {
        const { positional: [command, target], flags } = parse(argv);
        validateReply(flags, command);
        assert(flags.config, '--config is required.');
        phase = 'configuration';
        const loaded = await loadConfig(flags.config, { signal: controller.signal });
        if (command === 'config' && target === 'inspect') {
            phase = 'operation';
            await writeStream(stdout, `${JSON.stringify(inspectConfiguration(loaded.config))}\n`, controller.signal); return 0;
        }
        phase = 'startup';
        runner = await createConfiguredRunner(loaded.config, { baseDir: loaded.baseDir, signal: controller.signal });
        phase = 'operation';
        if (command === 'serve' || command === 'listen') {
            const sources = command === 'listen' ? { [target]: runner.sources[target] } : runner.sources;
            assert(Object.values(sources).every(Boolean), 'Incoming connection not found.');
            host = await createHost({ ...loaded.config.host, runner, sources, signal: controller.signal,
                role: flags.role ?? loaded.config.host?.role ?? 'all', onError: error => writeStream(stderr, `${JSON.stringify(error)}\n`, controller.signal) });
            await new Promise(resolve => { if (controller.signal.aborted) resolve(); else controller.signal.addEventListener('abort', resolve, { once: true }); });
            return 130;
        }
        const input = async () => readJson(flags.input ?? '-', { stdin, signal: controller.signal, maxBytes: 1048576 });
        const wrap = value => {
            const submission = flags.flow ? bindInput(value, { flow: flags.flow, inputMode: flags.kind ?? 'submission' }) : value;
            if (flags.reply === 'final') {
                const definition = Object.hasOwn(loaded.config.flows, submission?.flow) ? loaded.config.flows[submission.flow] : null;
                assert(definition, 'Reply requires a configured flow.'); validateReply(flags, command, definition);
            }
            return submission;
        };
        const runOptions = { source: 'cli', signal: controller.signal, waitFor: flags.wait ?? 'required', waitMs: Number(flags['wait-ms'] ?? 60000) };
        if (command === 'batch') {
            const stream = !flags.input || flags.input === '-' ? stdin : createReadStream(flags.input);
            let failed = false;
            for await (const line of jsonLines(stream, { signal: controller.signal, maxBytes: 1048576, recoverRecords: true })) {
                let response;
                try {
                    assert(!line.error, line.error?.message, line.error?.code);
                    const value = await runner.run(wrap(line.value), runOptions);
                    if (!['completed', 'running'].includes(value.status)) failed = true;
                    response = { line: line.line, ok: true, value: await output(value, flags, runner) };
                } catch (error) { failed = true; response = { line: line.line, ok: false, error: publicError(error) }; }
                await writeStream(stdout, `${JSON.stringify(response)}\n`, controller.signal);
            }
            return failed ? 1 : 0;
        }
        if (command === 'export') {
            for await (const bytes of runner.exportTask(target, { signal: controller.signal })) await writeStream(stdout, bytes, controller.signal);
            return 0;
        }
        let value;
        const options = flags.options ? await readJson(flags.options, { signal: controller.signal }) : {};
        if (['run', 'submit', 'preview'].includes(command)) value = await invoke(runner, command,
            { input: wrap(await input()), options: command === 'run' ? runOptions : {} }, { source: 'cli', signal: controller.signal });
        else if (command === 'recover' || command === 'maintain') value = await runner[command]();
        else if (command === 'pause' || command === 'resume') value = await runner.pause(options.type ?? 'flow', target, command === 'pause');
        else {
            const action = ({ status: 'get', retry: 'retryDelivery', resolve: 'resolveDelivery', 'resolve-evaluation': 'resolveEvaluation', completion: 'recordCompletion', export: 'exportTask', import: 'importTask' })[command] ?? command;
            const selectedOptions = { ...options, ...(flags.flow ? { flow: flags.flow } : {}) };
            if (flags.reply === 'final') {
                const taskId = ['retry', 'resolve', 'completion'].includes(command) ? target?.split(':delivery:')[0] : target;
                const saved = taskId ? await runner.store.get(taskId, runner.namespace, { metadata: true }) : null;
                const flow = selectedOptions.flow ?? saved?.task.flow;
                const definition = ['reprocess', 'redeliver', 'reevaluate', 'import'].includes(command)
                    ? Object.hasOwn(loaded.config.flows, flow) && loaded.config.flows[flow] : saved ? await readPart(runner, taskId, ['plan']) : null;
                assert(definition, 'Reply requires an available execution flow.'); validateReply(flags, command, definition);
            }
            if (command === 'import') {
                const chunks = importSource(flags.input, stdin, controller.signal);
                value = await runner.importTask(chunks, { ...selectedOptions, signal: controller.signal });
            } else value = await invoke(runner, action, { taskId: target, deliveryId: target, options: selectedOptions,
                ...(['resolve', 'resolve-evaluation'].includes(command) ? { resolution: await input() } : {}), ...(command === 'completion' ? { completion: await input() } : {}),
                ...(command === 'list' ? options : {}) }, { source: 'cli', signal: controller.signal });
        }
        await writeStream(stdout, `${JSON.stringify(await output(value ?? null, flags, runner))}\n`, controller.signal);
        return value?.status && ['failed', 'partial', 'unknown', 'cancelled'].includes(value.status) ? 1 : 0;
    } catch (error) {
        const primary = error instanceof AggregateError && error.cause !== undefined ? error.cause : error;
        if (!interrupted) await bounded(signal => writeStream(stderr, `${JSON.stringify({ ok: false, error: publicError(primary) })}\n`, signal),
            { timeoutMs: 1000, stage: 'diagnostics' }).catch(() => {});
        return interrupted ? 130 : failureExitCode(primary, phase);
    } };
    let exitCode;
    try {
        exitCode = await operation();
        await closeEvery([() => host?.close(), () => runner?.close({ cancel: interrupted })], { sequential: true });
    } catch (error) {
        if (!interrupted) await bounded(signal => writeStream(stderr,
            `${JSON.stringify({ ok: false, error: publicError(cleanupDiagnostic(error)) })}\n`, signal),
        { timeoutMs: 1000, stage: 'diagnostics' }).catch(() => {});
        exitCode = interrupted ? 130 : exitCode || 1;
    } finally {
        process.removeListener('SIGINT', stop); process.removeListener('SIGTERM', stop);
    }
    return exitCode;
}
