import { isRecord } from '../util.mjs';

// Keep credential discovery at the application boundary. SDK constructors otherwise
// consult process.env, including when a configured runner supplies an isolated env.
const providers = Object.freeze({
    jev: ['TYPESAFE_API_KEY', 'https://api.typesafe.ai'],
    'openai-decisions': ['OPENAI_API_KEY', 'https://api.openai.com'],
    'openai-compatible': ['OPENAI_API_KEY', 'https://api.openai.com'],
    'gemini-decisions': ['GEMINI_API_KEY', 'https://generativelanguage.googleapis.com'],
    'openrouter-decisions': ['OPENROUTER_API_KEY', 'https://openrouter.ai'],
    'openrouter-extraction': ['OPENROUTER_API_KEY', 'https://openrouter.ai'],
    openrouter: ['OPENROUTER_API_KEY', 'https://openrouter.ai'],
    anthropic: ['ANTHROPIC_API_KEY', 'https://api.anthropic.com'],
    'cloudflare-decisions': ['CLOUDFLARE_API_TOKEN', 'https://api.cloudflare.com'],
    'cloudflare-extraction': ['CLOUDFLARE_API_TOKEN', 'https://api.cloudflare.com'],
});

function environmentValue(name, env) {
    const scoped = env[`DECISION_BRIDGE_${name}`];
    return scoped === undefined ? env[name] : scoped;
}

/** Preserve explicit credentials and fail closed on missing or invalid defaults. */
export function providerCredentials(settings, env) {
    if (!isRecord(settings) || !Object.hasOwn(providers, settings.type)) return settings;
    if (settings.type.startsWith('cloudflare-') && settings.transport === 'binding') return settings;
    const [name, origin] = providers[settings.type];
    let official = settings.baseURL === undefined;
    if (!official) {
        // The SDK still owns endpoint validation; never send defaults to another origin.
        try { official = new URL(settings.baseURL).origin === origin; } catch { /* SDK rejects malformed endpoints. */ }
    }
    return { ...settings,
        ...(settings.apiKey === undefined ? { apiKey: official ? environmentValue(name, env) ?? '' : '' } : {}),
        ...(settings.type.startsWith('cloudflare-') && settings.accountId === undefined
            ? { accountId: official ? environmentValue('CLOUDFLARE_ACCOUNT_ID', env) ?? '' : '' } : {}),
    };
}

/** Resolve lazy route configs now, without changing routing policy or readiness. */
export function routingCredentials(settings, env) {
    if (!isRecord(settings) || !isRecord(settings.routes)) return settings;
    return { ...settings, routes: Object.fromEntries(Object.entries(settings.routes).map(([name, route]) =>
        [name, isRecord(route) && route.config !== undefined
            ? { ...route, config: providerCredentials(route.config, env) } : route])) };
}
