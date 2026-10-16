# Raft Toy

A small, readable implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf)
that replicates a key-value store across a cluster of simulated nodes. It includes tests for
the failure modes that matter and an interactive UI.

## Quick start

```sh
npm install
npm test          # run the test suite
npm run typecheck # type-check without emitting
```

## Design

_TODO: deterministic simulated network, pure node state machine, persistence model._

## What's tested

_TODO_

## Out of scope

- Log compaction / snapshots
- Dynamic membership changes (joint consensus)
- Exactly-once client semantics (request deduplication)
