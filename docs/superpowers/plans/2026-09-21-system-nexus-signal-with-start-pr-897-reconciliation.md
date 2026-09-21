# System Nexus Signal-With-Start Reconciliation

**Branch:** `test/system-nexus-signal-with-start`
**Compared with:** [draft PR #897](https://github.com/temporalio/features/pull/897)
**Status:** Implemented locally; commit requested; no push planned.

## Goal

Keep the basic workflow-side `SignalWithStartWorkflowExecution` contract small,
explicit, and testable across Go, Python, and .NET. Defer advanced payload
converter, codec, and external-storage behavior until each SDK can support the
System Nexus envelope path reliably.

## Review summary

PR #897 combines two concerns:

1. Basic Signal-With-Start behavior: start a target workflow, signal it twice,
   and reuse the existing execution on the second call.
2. Advanced serialization behavior: custom converters/codecs and Python
   external storage for inner payloads.

The draft also changes the shared runner to globally pin an experimental CLI
server build. Its Python coverage passes CI, but its .NET feature test fails
when the SDK sees the `SignalWithStartWorkflowExecutionResponse` System Nexus
envelope and eventually times out.

The reconciliation keeps the basic behavior, adds Go request/history checks,
and makes the advanced matrix an explicit follow-up.

## Implemented changes

### Go request semantics

- Set `WorkflowIdConflictPolicy` to
  `WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING`.
- Leave the low-level request namespace unset so the caller workflow namespace
  is used.
- Add a request-construction test covering workflow ID, type, task queue,
  signal, payload plumbing, policy, and omitted namespace.
- Retain the SDK capability guard: Go SDK v1.49.0 rejects the reserved
  `__temporal_system` endpoint, so the harness reports a skip until the SDK
  exposes the workflow-side endpoint.
- Verify response `started` flags, stable target run ID, target result, and
  caller/target history event shapes.

### Python basic coverage

- Add `TargetWorkflow` and `CallerWorkflow` using the public
  `workflow.signal_with_start_workflow` API.
- Use `WorkflowIDConflictPolicy.USE_EXISTING` for both calls.
- Pass a deliberately unused second start value and assert the original start
  value plus both signals in order.
- Do not configure a custom converter, codec, or external-storage driver.

### .NET basic coverage

- Add typed `Workflow.SignalWithStartWorkflowAsync` calls with
  `WorkflowIdConflictPolicy.UseExisting`.
- Register the caller and target workflows through the standard feature
  harness.
- Assert the original start value and both ordered signals.
- Do not configure `ConfigureClient` or copy the draft PR's advanced codec path.

### Documentation and scope

- Document the shared contract, endpoint/service, same-namespace behavior,
  language-specific assertions, and Go SDK limitation.
- State clearly that advanced converter/codec/external-storage checks are
  outside this feature and require a separate follow-up.
- Leave the shared runner and protected feature registry/config files unchanged.

## Verification plan

Run the available checks before committing:

```bash
GOCACHE=/private/tmp/temporal-features-gocache go test ./...
GOCACHE=/private/tmp/temporal-features-gocache go -C features test ./...
GOCACHE=/private/tmp/temporal-features-gocache go -C harness/go test ./...
gofmt -d features/system_nexus/signal_with_start/feature.go \
  features/system_nexus/signal_with_start/feature_test.go
git diff --check
```

When the required tools and network permissions are available, also run:

```bash
uv run ruff check --select I features/system_nexus/signal_with_start/feature.py
uv run ruff format --check features/system_nexus/signal_with_start/feature.py
dotnet build dotnet.csproj -p:TemporalioVersion=1.19.0
go run . run --lang python --version 1.33.0 system_nexus/signal_with_start
go run . run --lang cs --version 1.19.0 system_nexus/signal_with_start
```

## Verification result

- Root Go, features Go, and Go harness suites pass.
- Go formatting and whitespace checks pass.
- Independent review found no Critical, Important, or Minor findings.
- Python Ruff/MyPy/import checks against SDK 1.33.0 passed in review tooling.
- Local Python E2E, .NET build/formatting, and all embedded-server E2E runs
  remain environment-blocked: `uv` and `dotnet` are unavailable, and the
  sandbox rejects the embedded server's `[::1]:0` bind.
- No commit or push occurs until explicitly requested.

## Follow-up: advanced payload matrix

Create a separate feature or plan for custom payload converters, codecs, and
external storage. Keep the successful Python assertions from PR #897 as a
starting point, but do not enable the .NET version until the SDK can traverse
the System Nexus response envelope without treating it as an application
payload.
