# BEAD-RUNBOOK.md

## Purpose
Beads are small, self-contained units of work defined in HELIX-ONEIRON-SPEC-v1.1.1.md. Each bead captures a discrete task with clear acceptance criteria so work can be parallelized, verified, and resumed predictably.

## Bead Definition
A bead entry typically includes:
- id, name, type, dependencies, estimated_complexity
- tasks: the exact work items to implement
- acceptance: how completion is verified
- files_touched: expected files to change
- codex_prompt: the prompt to execute the bead

## Waves and Ordering
Beads are grouped into waves (e.g., -1 through 5). Dependencies gate execution. Within a wave, beads marked parallel can run concurrently; otherwise, execute sequentially.

## Execution Flow
1. Read the bead definition and dependencies.
2. Re-check Oneiron invariants and AGENTS.md rules.
3. Run the bead (choose the appropriate mode):

```bash
# Research bead
codex exec "<task prompt>" --json -o prose/out/bead-X.json

# Implementation bead
codex exec --full-auto "<task prompt>" \
  --output-schema prose/schemas/bead-result.schema.json \
  -o prose/out/bead-X.result.json
```

4. Implement the tasks and add tests:
   - HQL features: hql-tests/
   - Oneiron extensions: oneiron-tests/
5. For Rust changes, run: cargo fmt, cargo clippy, cargo test.
6. Validate acceptance criteria and record results in the bead output file.

## Failure and Resume
If tests fail or the bead is incomplete, resume with:

```bash
codex exec resume --last "Fix failing tests for bead X"
```

Re-run required tests and re-validate acceptance criteria.

## Outputs
- Research notes: prose/out/bead-X.json
- Implementation results: prose/out/bead-X.result.json (when the schema exists)
