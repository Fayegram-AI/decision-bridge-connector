# Development and verification

Run these commands from the Connector source checkout with Node 22.17 or later.
The checkout includes the tests and development scripts.

## Package output

`npm run build` runs `npm pack` into `release/` and verifies that archive in an installed
consumer. The archive contains JavaScript modules and TypeScript declarations from `src/`,
the CLI entry point, JSON schemas, examples, and documentation. npm installs Decision Bridge 0.1.8, AJV and
`es-module-lexer` as dependencies.

Use `npm run test:package` to check the current checkout in a temporary directory.
Use `npm run test:package -- --tarball PATH` to check a specific archive.

## Verification commands

```sh
npm ci --ignore-scripts
npm run verify
```

`verify` runs the source checks, functional tests, TypeScript checks, browser checks,
and installed-package tests. Run the worker and transfer scale tests separately.

```sh
npm run test:scale
node --expose-gc scripts/qualify-transfers.mjs
```

`npm test` runs the shared test aggregator through Node's explicit test runner. For service
coverage, set `DBC_POSTGRES_URL` and `DBC_REDIS_URL` to disposable PostgreSQL and Redis instances
before testing. A run without those settings does not cover the corresponding service cases.
The worker scale test uses temporary SQLite files and adds PostgreSQL when `DBC_POSTGRES_URL`
is set. It checks indexed query plans, runs one, two, and four workers against 100,000 retained
tasks and their jobs, and checks recovery after a worker exits during evaluation.
The transfer test streams a SQLite execution archive larger than 64 MiB between two stores.

CI defines Node 22.17 and Node 24 jobs on Linux, Windows, and macOS. A Windows Node 22.16
compatibility check verifies that spool sources reject legacy file-stat runtimes before claiming input.
A separate job runs the functional tests with PostgreSQL and Redis and the worker scale test
with SQLite and PostgreSQL.

The package-consumer check verifies the file inventory, installs into a temporary application
without optional peers, imports every public JavaScript entry point, and exercises CLI examples. It also
checks that repacking after example execution excludes generated databases and outputs.
Dependency installation prefers the npm cache and fetches missing metadata or packages from
the registry. With the production dependencies and metadata cached, use
`npm run test:package -- --offline` or `npm run build -- --offline` to require offline
installation. A clean `npm ci` alone may not cache all metadata needed by the consumer.
When adding an example, list its files explicitly in the manifest's `files` field.

## Testing a local Core package

To test against a locally built Decision Bridge archive, use an archive matching the exact
Core version in `package.json`. Install it locally and supply it to package verification:

```sh
npm install --no-save --package-lock=false ./core.tgz
npm run test:package -- --core-tarball ./core.tgz
npm run build -- --core-tarball ./core.tgz
```

The consumer installs both archives explicitly and checks the exact declared core
version before exercising exports and saved-result adapters. The manifest retains
an exact version dependency; filesystem paths are not written into it.
