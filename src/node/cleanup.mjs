// Shared cleanup is portable so Live and Node owners expose the same failure contract.
export { cleanupError, cleanupDiagnostic, startupCleanupError, closeEvery } from '../cleanup.mjs';
