# AGENTS.md

This repository uses a persistent two-agent loop for implementation and validation.

The goal is not to produce a merely plausible patch.
The goal is to implement the intended product behavior with the simplest robust change that remains correct outside the currently observed example.

Optimize for:
- root-cause fixes
- explicit invariants and contracts
- general solutions
- deterministic validation
- clear handoffs

Avoid:
- symptom patches
- narrow case-by-case branches
- regex-first parsing or validation
- speculative abstractions
- long unproductive repair loops

## Core rules

Do not rely on chat memory as the project source of truth.
The canonical project state lives in the shared files under `docs/agent-loop/`.
`docs/agent-loop/agent_context.md` is local to each working copy and is ignored by Git.

Before changing code, configuration, tests, or documentation, first present the
intended design and wait for explicit user approval. Read-only investigation is
allowed before approval. If implementation reveals a material design change,
pause and agree on the revised design before continuing.

Do not treat the current failing test, log line, stack trace, or user example as the full specification.
A passing reproducer is necessary, but it is not sufficient.

Do not stop at the first patch that “could be right”.
Prefer a fix that makes the bad state impossible, invalid, or centrally handled.

If the same repair strategy fails twice without materially improving validation, stop iterating on it.
Step back, re-localize, and reconsider the invariant, the edit location, or the design.

## Read order before any code changes

Always read these files in this order:

1. `docs/agent-loop/ideal_spec.md`
2. `docs/agent-loop/agent_context.md`, if present
3. `docs/agent-loop/validation.md`
4. the latest file in `docs/agent-loop/history/`
5. only then the relevant code, tests, configs, logs, and docs

If the local context is missing in a fresh working copy, finish reading the shared files, then create it before coding.
If `ideal_spec.md` or `validation.md` is missing, create it before coding.
Update these files when they are stale or contradicted by the current task.

## Canonical source of truth

Use this priority order:

1. the current user request
2. `docs/agent-loop/ideal_spec.md`
3. `docs/agent-loop/validation.md`
4. `docs/agent-loop/agent_context.md`
5. current code and existing tests

Do not treat current code as correct if it conflicts with the intended product logic.
Do not treat legacy tests as correct if they encode the bug or outdated behavior.
When code, tests, and intended behavior disagree, move code and tests toward intended behavior.

## Task framing before implementation

At the start of each task, derive and keep track of these four items:

- Goal: what must change
- Context: which files, modules, logs, specs, or examples matter
- Constraints: architecture, safety, compatibility, and style requirements
- Done when: what must be true before the task is complete

For trivial mechanical tasks, keep this lightweight.
For non-trivial tasks, do not code until these are clear.

## Required diagnosis before editing

For any task beyond an obvious mechanical fix, identify all of the following before making a non-trivial code change:

1. the observed behavior
2. the expected behavior
3. the broken invariant, contract, or assumption
4. the first layer where the incorrect state becomes possible
5. the set of input classes or execution paths likely affected
6. why the planned fix is general rather than tailored to one observed example

If you cannot state these clearly, keep investigating.
Do not patch blindly.

## Fix selection policy

Choose the fix location and shape deliberately.

- Prefer the deepest practical layer where the incorrect state first becomes representable.
- Prefer shared domain logic, canonical normalization, or boundary validation over downstream cleanup in one caller.
- Prefer correcting the decision rule over adding guards around its consequences.
- Prefer deleting wrong logic over stacking compensating logic on top of it.
- For ambiguous or multi-file bugs, consider at least one alternative fix strategy before editing.
- Choose the smallest change that fully enforces the intended invariant.

A fix is suspicious if its rationale is any of the following:

- “it makes the current test pass”
- “it handles the exact payload from the bug report”
- “it matches the current wording of the model output”
- “it suppresses the exception without fixing why the exception occurs”

## Generalization gate

Before you keep a patch, verify all of the following:

- The rule can be stated without referencing the current failing example.
- The rule applies to a class of inputs or states, not one literal value.
- Adjacent valid, invalid, empty, null, reordered, and boundary cases have been considered where relevant.
- The patch does not depend on incidental formatting, exact error text, or one observed call path.
- The fix would still make sense if the symptom appeared in a different file, order, or payload shape.

If any answer is “no”, redesign the patch.

## Anti-special-case policy

Do not add branches keyed to one literal, filename, prompt wording, stack-trace fragment, error message, or test fixture unless an authoritative rule explicitly requires that distinction.

A special case is allowed only when at least one of these is true:

- the product spec explicitly requires it
- an external standard or protocol explicitly requires it
- backward compatibility requires it and that compatibility contract is real, documented, and testable

When a special case is legitimate:

- encode it at the narrowest authoritative boundary
- document the reason in code or surrounding docs if it is not obvious
- add regression tests that prove the boundary is legitimate

“Observed in one failing case” is not a valid reason.

## Regex policy

Use regex only when the pattern is truly lexical, stable, and simpler than a parser, schema, tokenizer, or ordinary string logic.

Regex is usually appropriate for:

- stable token extraction
- narrow log parsing with fixed formats
- file naming conventions
- clearly bounded lexical rewrites

Regex is usually the wrong tool for:

- JSON, YAML, TOML, XML, HTML, CSV, URLs, SQL, code, or other structured formats
- nested or recursive grammars
- semantic validation of open-ended LLM output
- business rules that belong in typed logic
- recovering malformed machine output that should have been structured at generation time

If you still use regex, keep it narrow, anchored, documented, and covered by tests for boundary conditions, escaping, whitespace, and unicode where relevant.

## LLM integration policy

When model output is meant for downstream machine use:

- prefer structured outputs, tool calls, enums, typed schemas, or parser-friendly contracts
- validate deterministically at system boundaries
- keep prompts, schemas, validators, and product logic aligned
- define an explicit “cannot comply / empty / no-match / refusal” shape when incompatible input is possible
- prefer types generated from code or code generated from types over hand-maintained parallel schemas where possible

Do not ask a model for free-form text and then bolt regex cleanup onto it as the primary correctness mechanism.

If a model repeatedly fails in a specific way:

- add a small example or counterexample when that failure pattern is stable
- simplify the task shape or split the task into clearer subtasks
- improve the tool or schema contract before adding more prose instructions

Use deterministic code whenever deterministic code can solve the problem.
Do not replace a parser, schema, or normal program logic with LLM reasoning when the problem is structurally solvable.

## Under-specified tasks

When the issue statement or user request is incomplete:

- prefer the user request and `ideal_spec.md`
- use surrounding docs, interfaces, naming, and tests to infer the intended behavior
- do not infer a broad rule from one failing example
- make the smallest reasonable assumption that preserves reversibility
- record the assumption in local `agent_context.md`

## Context and architecture awareness

Before changing behavior, inspect enough surrounding code to understand:

- direct callers and callees
- data shapes and type expectations
- nullability, emptiness, and default semantics
- configuration or environment boundaries
- version or compatibility constraints
- where normalization, validation, and business rules already live

Shallow local edits at the symptom site are suspicious when the bug is introduced upstream.
Do not patch only the renderer, serializer, or UI layer when the domain state itself is wrong.

## System responsibility boundaries

- Give every component one explicit area of responsibility and one source of
  truth for the decisions it owns.
- Treat client code as untrusted. Client-side checks may improve user
  experience, but they do not replace server-side validation, authentication,
  or authorization.
- Keep edge components limited to transport concerns such as routing,
  connection termination, standard headers, request correlation, and general
  protective limits.
- Do not place business decisions or authoritative user, role, session, or
  permission state in edge components.
- Keep authentication, authorization, use-case execution, and consistency of
  state changes behind the authoritative application boundary.
- Use storage for durability and integrity constraints, not as an alternative
  interface that bypasses application behavior.
- Keep delivery, monitoring, and administrative components outside business
  request processing.
- Design responsibility transfers explicitly. Do not let the same decision
  gradually become duplicated across components.

## Internal architecture boundaries

- Keep process entrypoints limited to handing control to the composition root
  and handling process termination.
- Let the composition root construct and connect dependencies and own resource
  and process lifecycle without accumulating business rules.
- Let the transport layer own protocol parsing, input-shape validation,
  extraction of request-scoped data, and response formatting.
- Do not use transport models as domain entities or storage models.
- Let the application layer orchestrate use cases, define transaction
  boundaries, and coordinate modules.
- Let the domain layer own business state, valid transitions, and invariants
  without depending on transport, storage, or concrete infrastructure.
- Keep conversion between storage data and domain or read models inside the
  persistence boundary. Do not expose storage details outside that boundary.
- Let repositories participate in a supplied transaction without deciding the
  transaction boundary of a use case themselves.
- Keep shared infrastructure free of business meaning and independent from
  domain modules.
- Let each business module own its vertical slice of behavior. Avoid central
  transport, service, or repository modules that obscure that ownership.
- Connect modules through small, explicit contracts. Do not reach into another
  module's transport or persistence internals.
- Propagate request-scoped technical data and identity uniformly through the
  request context instead of extracting them repeatedly in handlers.
- Treat read models, domain entities, use-case commands, transport models, and
  storage models as distinct roles even when some fields coincide.
- Add an abstraction only for a stable boundary or demonstrated reuse.

## Search and edit discipline

- Read enough context before editing a file.
- Batch logically related edits together instead of thrashing with many tiny micro-patches.
- Reuse existing helpers and patterns before inventing new ones.
- Do not do broad search-and-replace across the repository without confirming each affected surface.
- Do not duplicate business rules across modules just to avoid understanding an existing abstraction.

## Commit messages

- Write commit titles in English using the Conventional Commits form.
- Write commit bodies in Russian as bullet points describing what changed or
  was added.
- Treat each bullet as its own text block. Use normal punctuation between
  sentences inside a bullet, but do not put a period after the final sentence
  of that bullet.
- For a `fix` commit, describe the problem in Russian prose before the bullets,
  then list what was fixed.

## General engineering principles

- Optimize for correctness, readability, and directness.
- Prefer the simplest design that fully solves the problem.
- Avoid speculative abstractions, framework-like indirection, and future-proofing that is not required by the task.
- Keep control flow flat and explainable.
- If the implementation is hard to explain in plain language, simplify it.
- Respect existing repository conventions unless they conflict with intended logic or clearly reduce code quality.

## Avoid overengineering

- Do not create extra layers, wrapper classes, factories, adapters, or generic helper modules unless they clearly improve reuse, isolation, boundary clarity, or testability.
- Do not split logic into many one-line pass-through helpers.
- Prefer one well-structured function or module over a swarm of tiny abstractions.
- Introduce a helper only when at least one of these is true:
  - logic is reused
  - a boundary becomes clearer
  - testing becomes materially easier
  - the code becomes simpler to read

## Deterministic code over heuristics

- Do not replace deterministic logic with LLM-based or heuristic logic when the problem can be solved reliably with normal code.
- If the input format is structured or standardized, prefer a real parser, schema, AST, or standard-library facility.
- Prefer parser/schema-based handling for formats such as:
  - JSON
  - YAML
  - TOML
  - URLs
  - XML / HTML
  - CSV
  - CLI arguments
  - timestamps and other structured tokens

## Comments, docstrings, and typing

- Comments must add information that is not obvious from the code.
- Do not narrate the code line by line.
- Use comments for:
  - invariants
  - domain rationale
  - tricky edge cases
  - external API quirks
  - performance constraints
  - security or safety constraints
- Keep inline comments rare and useful.
- Keep comments accurate; outdated comments are worse than no comments.
- Write docstrings for public modules, public classes, and public functions whose interface is part of the contract.
- Do not add boilerplate docstrings to trivial private helpers.
- Use type hints where they clarify interfaces and contracts.
- Do not annotate every local variable unless it improves readability or is required by the type checker.
- Do not add `from __future__ import annotations` by default.
- Use `from __future__ import annotations` only if:
  - the repository already standardizes on it
  - or the target Python version and typing/runtime constraints make it materially useful

## Error handling

- Keep error handling explicit.
- Do not swallow exceptions silently.
- Do not add broad catch-all handlers to hide incorrect behavior.
- Do not add success-shaped fallbacks that mask corruption, parse failures, or incompatible inputs.
- Surface or propagate failures in the way the repository already expects.

## Testing and validation philosophy

Passing one target test only proves plausibility.
The bar is semantic correctness relative to the intended behavior in the user request and `ideal_spec.md`.

When changing logic, validate at the narrowest relevant level first, then widen only as needed.

Regression tests should encode the intended invariant or contract, not merely the current implementation detail.

When relevant, test:

- the reproducer or failing behavior before the fix
- the direct happy path after the fix
- adjacent edge cases
- invalid or incompatible inputs
- null / empty / missing values
- ordering or duplication issues
- round-trip behavior for parse/serialize transformations
- compatibility behavior for public interfaces

## Patch-shape examples

Bad patch shapes:

- Add `if value == "X"` because one failing case used `"X"`.
- Extract JSON from model prose with regex and hope the parser succeeds.
- Silence an exception or return a default value without fixing why the state became invalid.
- Patch one output surface while the same bad state can still propagate elsewhere.

Good patch shapes:

- Normalize or validate the full input class at the authoritative boundary.
- Replace free-form model output with a schema or tool contract and deterministic parsing.
- Fix the invariant where the invalid state is introduced, then add regression coverage for adjacent cases.
- Reuse or strengthen a shared helper so all callers become correct together.

## Two-agent loop

This repository intentionally uses two roles: implementer and validator.

### Implementer responsibilities

The implementer should:

- reconstruct the intended product logic first
- identify the broken invariant and the correct fix layer
- choose the smallest robust change
- run the smallest relevant checks after each meaningful milestone
- update `agent_context.md`
- add a shared history entry when required by [Required outputs before finishing](#required-outputs-before-finishing)

Use the `agent-loop-implement` skill for implementation, debugging, or feature work when appropriate.

### Validator responsibilities

The validator is not a rubber stamp.
The validator should assume the first patch may be plausible but wrong.

The validator should actively search for:

- symptom-only fixes
- overfitting to one test or one example
- hidden special cases
- regex misuse
- incorrect localization
- missed callers, callees, or state transitions
- type and data-shape mismatches
- compatibility regressions
- insufficient validation
- architectural inconsistency with the intended logic

The validator should try to falsify the patch with adjacent cases and alternative execution paths.
If the patch still looks correct, the validator should be able to explain why it generalizes.

Use the `agent-loop-validate` skill for independent review, bug hunting, regression checks, and repair when appropriate.

## Operating workflow

- Reconstruct the intended product logic before changing code.
- For complex or ambiguous work: explore, then plan, then implement.
- For small, obvious one-diff tasks: keep the proposed design brief, but still
  wait for explicit approval before editing.
- Search just deep enough to identify the correct edit surface; do not wander indefinitely.
- After each meaningful milestone, run the smallest relevant checks from `docs/agent-loop/validation.md`.
- If validation does not improve after repeated attempts on the same strategy, stop and re-localize.
- Do not finish in a “probably fixed” state.

## Validation policy for prompts, tools, and agent behavior

If the task changes prompt logic, tool contracts, parsing strategy, or model interaction boundaries:

- add or update eval-style regression cases where practical
- test both valid and incompatible inputs
- test failure handling, refusal handling, and incomplete-output handling where applicable
- verify that tool names, parameter names, and descriptions make misuse harder, not easier
- prefer contract changes that reduce ambiguity rather than post-hoc cleanup

## Stable vs changing context

Keep this file stable, durable, and high-signal.
Do not store session-specific state here.

Store changing state in:

- `docs/agent-loop/agent_context.md` for local working notes and debugging details
- `docs/agent-loop/history/*.md` for shared handoffs

When this file starts accumulating many task-specific procedures, move those procedures into:

- repository docs under `docs/agent-loop/` or another appropriate docs location
- dedicated skills for repeatable workflows
- narrowly scoped nested `AGENTS.md` or override files closer to specialized subdirectories

## When to use skills

Use skills for workflows that are repeated, specialized, or benefit from fixed deterministic steps.

Rules for skills:

- one skill should do one job
- keep inputs and outputs explicit
- include scripts or assets only when they materially improve reliability
- use skills to hold detailed procedures that would otherwise bloat this file

If the agent makes the same mistake twice, decide whether the fix belongs in:

- this file, if it is a stable repository-wide rule
- a skill, if it is a repeatable workflow
- a task-specific doc, if it is volatile or domain-local

## Required outputs before finishing

Update the local `docs/agent-loop/agent_context.md` in each coding session. Add a new file in
`docs/agent-loop/history/` when a work slice is complete or a shared contract changes.

Before finishing a session, confirm that all applicable items are complete.

1. changed code is validated with relevant checks
2. `agent_context.md` reflects the actual current state
3. a shared history entry is added if the session completed a work slice or changed a shared contract
4. open risks are written explicitly
5. the validator role has either accepted the fix or recorded concrete objections

## Suggested minimum handoff content

Each shared handoff should summarize the information other slices need.

- what changed and how it affects other slices or their shared contracts
- which scenarios were verified and the outcome
- remaining dependencies, questions, or integration risks

## Runtime validation policy (Docker Compose)

Agents may validate changes against the already running docker compose services.

Allowed operations:

- `docker compose ps`
- `docker compose config`
- `docker compose logs --tail=<n> <service>`
- `docker compose exec -T <service> <read-only or test command>`
- `docker compose exec -T <service> pytest ...`
- `docker compose exec -T <service> python -m pytest ...`
- `docker compose exec -T <service> bash -lc "<safe diagnostic or test command>"`
- targeted `docker compose build <service>`
- targeted `docker compose up -d <service>`
- targeted `docker compose restart <service>`

Rules:

- Prefer the smallest affected service only.
- Prefer in-container smoke tests over host-level ad hoc commands.
- Prefer commands listed in `docs/agent-loop/validation.md`.
- After any rebuild or restart, rerun the smallest relevant smoke test and inspect service logs.
- If a command is not explicitly allowed by this policy or may affect data or state broadly, ask first.

Forbidden unless the user explicitly requests it:

- `docker compose down`
- `docker compose down -v`
- `docker compose rm`
- `docker system prune`
- `docker volume rm`
- destructive database resets, downgrades, or wipes
- broad rebuild of the whole stack when only one service is affected
- changing secrets, env files, or external infra outside the local dev environment

## Maintainers of this file

When editing this file:

- prefer fewer, stronger rules over many weak reminders
- add rules only when they prevent a real repeated failure mode
- keep repository-wide rules here and move specialized procedures elsewhere
- keep this file high-signal; more prompt text is not automatically better
- preserve the focus on root cause, invariants, and semantic validation
- do not turn this file into session memory or a dumping ground for one-off incidents
