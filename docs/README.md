# Connector documentation

Decision Bridge Connectors 0.1.1 provides an embeddable execution SDK, transport adapters,
and a CLI/service host. Configuration, persisted stores, summaries, process envelopes, and
transfers use schema version 3. The package depends on Decision Bridge 0.1.8 through its
public exports.

| Task | Guide |
| --- | --- |
| Start with an embedded flow or offline CLI example | [Package README](../README.md) |
| Understand execution ownership, guarantees, scheduling, and recovery | [Architecture](ARCHITECTURE.md) |
| Use Bridge or an external evaluator; configure provider selection | [Evaluators and routing](EVALUATORS.md) |
| Choose persistence, inspect evidence, or transfer an execution | [Storage, inspection, and transfer](DATA.md) |
| Configure the CLI, workers, reload, or HTTP hosting | [Node CLI and service](NODE.md) |
| Receive tasks and correlate source contributions | [Incoming connections](INCOMING.md) |
| Map data, apply application policies, or use in-memory fan-in | [Policies and mappings](POLICIES.md) |
| Install integration peers or implement connectors, stores, and extensions | [Integration contracts](INTEGRATIONS.md) |
| Contribute changes or verify an archive | [Development and verification](DEVELOPMENT.md) |
| Find example configuration requirements | [Examples](../examples/README.md) |
| Review version changes | [Changelog](../CHANGELOG.md) |

The SDK defaults to memory; durable storage is configured separately. A flow selects an
evaluator explicitly. Built-in automatic provider/model selection requires a routed evaluator and
a declared routing policy. Importing the SDK does not start a CLI, listener, or worker loop.
