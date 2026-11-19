// A workflow process reads one envelope and writes one JSON receipt, with no shell.
import { stdin, stdout } from 'node:process';
const chunks = []; let size = 0;
for await (const chunk of stdin) {
    size += chunk.length;
    if (size > 1048576) throw new Error('Input too large');
    chunks.push(chunk);
}
const envelope = JSON.parse(Buffer.concat(chunks).toString('utf8'));
stdout.write(JSON.stringify({ status: 'acknowledged', receipt: { response: { reviewed: true, deliveryId: envelope.deliveryId } },
    completion: { status: 'completed', scope: 'example-process' } }));
