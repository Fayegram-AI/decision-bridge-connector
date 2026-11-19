import { createRunner } from '../../src/index.mjs';
import { serveMcpStdio } from '../../src/integrations/mcp-service.mjs';

// Deliberately stdout-clean: the official SDK owns protocol framing.
const runner = createRunner({ validators: { v: { revision: '1', validate: value => value } },
    flows: { test: { revision: '1', inputs: ['result'], validator: 'v' } } });
const server = await serveMcpStdio({ runner, flows: ['test'] });
server.onclose = () => { void runner.close(); };
