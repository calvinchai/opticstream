---
title: Prefect Variable–backed project state
---

## Prefect Variable–backed project state

This module provides a layered state-management pattern for pipeline state persisted in a Prefect Variable. It is used by pipelines such as the LSM state service to track project-level progress and metadata without requiring a separate database.

## Design overview

This module uses a layered state-management pattern for pipeline state persisted in a Prefect Variable.

The goals are:
- keep the domain model easy to reason about
- make reads vs writes explicit
- ensure exclusive mutation with Prefect concurrency locks
- support both authoritative locked reads and best-effort unlocked reads
- avoid duplicating many narrow service methods like `mark_done`, `set_uploaded`, etc.

The design separates generic infrastructure from pipeline-specific state.

### Why this pattern exists

A Prefect Variable is a simple persistence mechanism, but it does not provide transactional updates by itself. If multiple workers load, mutate, and save the same Variable concurrently, updates can be lost.

To make Variable-backed state safe, every mutation must follow this pattern:
1. acquire a project-scoped lock
2. load the current state from the Prefect Variable
3. mutate the in-memory model
4. save the updated state back to the Prefect Variable
5. release the lock

This module wraps that pattern in a reusable store and exposes a cleaner domain API on top.

### High-level architecture

There are two layers.

1. Generic infrastructure layer

This layer is reusable across different pipelines. It does not know what a strip, slice, or channel is.

It provides:
- a repository abstraction for loading/saving project state
- a lock abstraction for exclusive access
- a generic `BaseProjectStateStore[TState]`
- generic access helpers:
  - `locked(...)`
  - `read(...)`
  - `update(...)`
  - `peek(...)`
  - `edit(...)`

This layer is responsible for Prefect integration.

2. Pipeline-specific domain layer

This layer defines the actual state tree and behavior for a given pipeline.

For the LSM pipeline, that includes:
- view models for readonly access
- mutable models for in-place mutation
- hierarchy navigation such as:
  - `get_or_create_slice`
  - `get_or_create_channel`
  - `get_or_create_strip`
- domain mutations such as:
  - `mark_started`
  - `mark_completed`
  - `set_uploaded`
  - `set_backed_up`

This layer is responsible for state semantics.

### Prefect Variable persistence model

Project state is persisted as JSON in a Prefect Variable, keyed by project name.

A typical lifecycle is:
- the repository loads the Variable into a mutable Pydantic model
- mutations happen in memory
- the model is serialized back to JSON and stored into the same Variable

This gives a simple durable backing store without requiring a separate database.

Because the Prefect Variable is shared, mutation must always happen under a project-scoped lock.

### Locking model

State mutation is coordinated by a Prefect global concurrency limit with limit 1 per project.

That lock is used to guarantee exclusive access during:
- load
- mutate
- save

This prevents two workers from overwriting each other’s changes.

The lock name is project-specific, so unrelated projects can still update their own state concurrently.

### Mutable models vs readonly views

Each state object has two forms.

**Readonly view model**

The view model is frozen and intended for inspection only.

Examples:
- `LSMProjectStateView`
- `LSMSliceStateView`
- `LSMChannelStateView`
- `LSMStripStateView`

These are returned from readonly APIs.

**Mutable model**

The mutable model inherits the fields of the view model and adds mutation behavior.

Examples:
- `LSMProjectState`
- `LSMSliceState`
- `LSMChannelState`
- `LSMStripState`

These are only exposed inside mutation scopes.

Each mutable model provides `to_view()` so it can be converted to a readonly snapshot safely.

This split prevents callers from accidentally mutating state outside the proper locked update flow.

### Store API

The generic store provides a few core operations.

**`locked(project_name, ...)`**

A low-level context manager that:
- acquires the project lock
- loads the current state
- yields the mutable state
- saves the state on exit

This is the foundation for all locked operations.

**`read(project_name, reader, ...)`**

Runs a readonly callback under the lock and returns its result.

Use this when the read must be consistent with concurrent writers.

**`update(project_name, mutate, ...)`**

Runs a mutation callback under the lock.

Use this when a caller only needs a single callback-based update.

**`peek(project_name, reader)`**

Loads state without locking and returns the callback result.

Use this only for best-effort inspection where races are acceptable.

**`edit(project_name, getter, ...)`**

A generic context manager that yields a selected mutable object from the loaded state.

This is the primitive used to build pipeline-specific context managers such as `open_strip(...)`.

### Domain service API

The pipeline-specific service presents a simpler API for flows and CLIs.

It uses three families of methods.

**`open_*`**

Examples:
- `open_project`
- `open_slice`
- `open_channel`
- `open_strip`

These are context managers for mutable access.

They:
- lock the project state
- load the state
- get or create the requested object
- yield the mutable model
- save on exit

Example:

```python
with LSM_STATE_SERVICE.open_strip(
    project_name,
    slice_id=1,
    strip_id=2,
    channel_id=0,
) as strip:
    strip.mark_completed()
    strip.set_uploaded(True)
```

This is the preferred mutation pattern.

**`read_*`**

Examples:
- `read_project`
- `read_slice`
- `read_channel`
- `read_strip`

These return readonly view models under the lock.

Use them when the read result matters for correctness.

**`peek_*`**

Examples:
- `peek_project`
- `peek_slice`
- `peek_channel`
- `peek_strip`

These return readonly view models without locking.

Use them for:
- status display
- logging
- debugging
- best-effort UI inspection

Do not use `peek_*` for check-then-act workflows.

### Why `open_*` instead of many narrow service methods

A service API with methods like:
- `mark_strip_completed(...)`
- `set_strip_uploaded(...)`
- `set_strip_backed_up(...)`

quickly becomes repetitive.

This pattern instead exposes the mutable model directly inside a safe mutation scope:

```python
with service.open_strip(...) as strip:
    strip.mark_completed()
    strip.set_uploaded(True)
```

Benefits:
- less boilerplate
- mutation logic stays on the model
- easier to combine multiple changes in one locked transaction
- fewer service methods to maintain

### Read and write semantics

This design makes read/write behavior explicit.

**Mutation path**

Use `open_*` or `update(...)`.

These may create missing objects and always operate under the lock.

**Locked readonly path**

Use `read_*`.

These do not create missing objects and return frozen view models.

**Unlocked readonly path**

Use `peek_*`.

These do not create missing objects and return frozen view models, but they are only snapshots and may already be stale when used.

### Separation of responsibilities

**Generic layer responsibilities**
- lock acquisition
- state loading and saving
- Prefect Variable integration
- generic editing and reading primitives

**Domain layer responsibilities**
- state fields and hierarchy
- domain mutations
- hierarchy navigation
- conversion to readonly views
- pipeline-specific naming and service methods

This separation keeps the generic code reusable and the domain code focused.

### Usage guidance

Use `open_*` when changing state:

```python
with service.open_strip(...) as strip:
    strip.mark_started()
```

Use `read_*` for authoritative decisions:

```python
strip = service.read_strip(...)
if strip is not None and strip.finished:
    ...
```

Use `peek_*` only for inspection:

```python
strip = service.peek_strip(...)
```

Do not do this:

```python
if not service.peek_strip(...).finished:
    service.open_strip(...)
```

because another worker may update the state between the peek and the write.

### Summary

This pattern treats Prefect Variable state as a small shared state store and adds the missing safety and structure around it:
- Prefect Variable provides persistence
- Prefect concurrency provides exclusive mutation
- generic store code provides reusable lock/load/save primitives
- pipeline models provide domain structure and behavior
- readonly views provide safe inspection
- `open_*`, `read_*`, and `peek_*` provide a clear API for users

The result is a state system that is simple, explicit, and safe to use in concurrent Prefect workflows.

