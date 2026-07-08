//! Library half of the worker daemon. Holds the pieces the `worker` binary
//! wires together but that are worth unit-testing on their own — currently
//! the per-graph [`tool_provider::DbToolRegistryProvider`].

pub mod reaper;
pub mod tool_provider;
