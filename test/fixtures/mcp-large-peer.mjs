import { createRunner, createMemoryStore } from '../../src/index.mjs';
import { serveMcpStdio } from '../../src/integrations/mcp-service.mjs';

const runner = createRunner({ store: createMemoryStore({ maxBytes: 2097152 }),
    validators: { v: { revision: '1', validate: value => value } },
    flows: { a: { revision: '1', inputs: ['result'], validator: 'v' } } });
await serveMcpStdio({ runner, flows: ['a'], maxBytes: 2097152 });
