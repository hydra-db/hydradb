<!-- Delete any section that does not apply. -->

## What this changes and why

<!-- The reasoning that is not visible in the diff: what was broken, what else
you considered, why this. If it closes an issue, say "Fixes #123". -->

## How it was verified

<!-- What you actually ran, not what you intended to run. If you ran only part
of `just ci` — it is a long run, 25m 41s clean — say which part. -->

- [ ] `just ci` passes locally (or `bash scripts/ci_local.sh`, which delegates to it)
- [ ] New behaviour has a test that fails without the change
- [ ] Ran `just smoke`, and `bash scripts/runtime_smoke.sh` if this touches the server runtime

## Compatibility

<!-- These decide whether the change can land in a patch release. If you tick
one, describe the upgrade path for an operator on the previous release. -->

- [ ] Changes the result of a query that works today
- [ ] Changes the Bolt or HTTP wire format
- [ ] Changes the on-disk or object-store layout, or needs a migration
- [ ] Adds or renames configuration, environment variables, or chart values
- [ ] Adds a third-party dependency
- [ ] None of the above

## Notes for the reviewer

<!-- Where to start, what you are unsure about, what you left out of scope.
Follow-up work belongs in an issue, linked from here. -->
