# System Nexus Signal-With-Start Feature Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Go feature test proving that a workflow can use the reserved System Nexus operation to signal a workflow in the same namespace, starting that target when absent and reusing it when already running.

**Architecture:** Add one feature under `features/system_nexus/signal_with_start`, deliberately outside `features/nexus` so the runner does not provision a user Nexus endpoint. A caller workflow invokes the generated `SignalWithStartWorkflowExecution` operation twice against one target workflow ID; the target waits for both signals, while result and history checks prove the first call started the target and the second reused the same run. A per-feature run variant enables the server-side feature flag on the embedded dev server.

**Tech Stack:** Go 1.26, Temporal Go SDK workflow APIs, generated `go.temporal.io/api` WorkflowService Nexus reference, Temporal feature harness, embedded Temporal dev server, JSON feature configuration.

**Spec:** `features/system_nexus/signal_with_start/README.md`; server implementation in `/Users/timli/Desktop/temporal/chasm/lib/workflow/nexus_service.go`; server integration coverage in `/Users/timli/Desktop/temporal/tests/signal_with_start_from_workflow_test.go`.

## Global Constraints

- Do not commit or push unless the user explicitly requests it.
- Keep the feature under `features/system_nexus/signal_with_start`; placing it under `features/nexus` would incorrectly create a user Nexus endpoint.
- Target endpoint `__temporal_system`, service `temporal.api.workflowservice.v1.WorkflowService`, and operation `SignalWithStartWorkflowExecution` exactly.
- Omit `SignalWithStartWorkflowExecutionRequest.Namespace` so the test proves that the server derives the target namespace from the caller workflow.
- Do not use `go.temporal.io/sdk/internalbindings`, `go:linkname`, reflection, or manually completed workflow tasks. The feature must exercise the public SDK workflow path.
- Do not add custom codec or external-storage coverage to this feature. Nested system-payload visitation is a separate concern and deserves a separate feature once the basic workflow-to-workflow path is running.
- Use only the embedded dev server for this feature while its `config.json` carries `runVariants`; the runner cannot apply per-feature dynamic config to an externally managed server.
- Do not generate a stored history for this feature: the runner rejects `--generate-history` for features with `runVariants`.
- The current pinned Go SDK v1.49.0 skips at runtime because `workflow.NewNexusClient` rejects the reserved `__temporal_` prefix. The feature must detect that capability and report a harness skip, not panic. The same code begins executing when a public SDK release permits `__temporal_system`.

---

## Verified Context

### Backend implementation

The implementation is present in `/Users/timli/Desktop/temporal` and originated in commit `01aa279c462fd9e7efc8e0ba6bbc4554b51557dd` (`Implement SignalWithStart as a system nexus endpoint (#9833)`). The current checkout is `fix/signalwithstart-continue-as-new-backoff` at `75a92d9175608450017c218407fceec440596f29`.

The end-to-end path is:

1. The caller workflow schedules a Nexus operation against `__temporal_system`.
2. History recognizes the reserved endpoint and routes it to the System Nexus processor rather than resolving a user-created Nexus endpoint (`chasm/lib/workflow/nexus_commands.go:47`; legacy HSM equivalent at `service/history/hsm/nexusoperations/workflow/commands.go:55`).
3. `SignalWithStartOperationProcessor.ProcessInput` fills an omitted namespace from the caller context, rejects a different namespace, injects the command request ID and workflow links, validates the request, and routes on caller namespace ID plus target workflow ID (`chasm/lib/workflow/nexus_service.go:75`).
4. The registered synchronous handler calls History's normal `SignalWithStartWorkflowExecution` implementation (`chasm/lib/workflow/nexus_service.go:24`).
5. The normal API signals a running target or creates and signals a new target (`service/history/api/signalwithstartworkflow/signal_with_start_workflow.go:37`).
6. The synchronous Nexus result carries `RunId`, `Started`, and `SignalLink`; failures become Nexus operation failures recorded in the caller history (`chasm/lib/workflow/nexus_service.go:47`; `chasm/lib/nexusoperation/invocation.go:233`).

The feature is gated by namespace dynamic config `history.enableSignalWithStartFromWorkflow`, whose default is `false` (`common/dynamicconfig/constants.go:3376`). The test must therefore enable it explicitly.

### SDK compatibility boundary

The feature repository pins Go SDK v1.49.0 and API v1.63.5. API v1.63.5 already provides:

```go
workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName
workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.SignalWithStartWorkflowExecution
workflowservice.SignalWithStartWorkflowExecutionRequest
workflowservice.SignalWithStartWorkflowExecutionResponse
```

Go SDK v1.49.0 and SDK main commit `626130f1fd9de50cfd90b884a3fb796504f22dc1` still reject `__temporal_` in public `workflow.NewNexusClient`. They contain an internal `NewSystemNexusClient`, but application and feature code must not depend on it. The backend's real-SDK integration test is skipped for the same reason at `tests/signal_with_start_from_workflow_test.go:678`.

The feature can still land safely now by probing the public constructor in `Feature.Execute`, recovering only its known reserved-prefix panic, and returning `runner.Skip(...)`. Once the public constructor accepts the system endpoint, the feature executes without a source change. When a released SDK adds support, the normal repository-wide SDK bump should update `go.mod`, `features/go.mod`, `harness/go/go.mod`, and their sums in its own dependency-update change.

### Existing feature-test patterns

- `features/nexus/sync_success/feature.go` shows a workflow-side synchronous Nexus call and verifies `NEXUS_OPERATION_SCHEDULED` then `NEXUS_OPERATION_COMPLETED`, with no `NEXUS_OPERATION_STARTED` event.
- `features/signal/signal_with_start/feature.php` verifies both semantic branches of Signal-With-Start: start-and-signal a missing workflow and signal an existing workflow.
- `features/child_workflow/signal/feature.go` registers caller and target workflows and waits for a target signal result.
- `features/signal/external/feature.go` shows custom `Execute` orchestration and a workflow that blocks on a signal.
- `features/data_converter/codec/feature.go` demonstrates payload inspection, but that scope is intentionally excluded here.
- `features/features.go` is the explicit Go feature registry.
- `cmd/run.go:180` expands `runVariants`, starts a fresh embedded dev server for each variant, and applies dynamic config overrides.
- `cmd/run.go:856` provisions Nexus endpoints only for feature paths beginning with `nexus/`; `system_nexus/` therefore correctly receives no user endpoint.

## Design Decisions

Use one target workflow and two sequential System Nexus calls from one caller workflow:

- Call 1 targets a missing workflow ID and must return `Started=true`.
- Call 2 targets the same workflow ID while the target is waiting and must return `Started=false`.
- Both responses must report the same non-empty run ID.
- The target begins with workflow input `1`, receives signal values `20` and `22`, and returns `43`.
- The caller history must contain two scheduled and two completed Nexus events, and no started Nexus events because this is a synchronous operation.
- The target history must start with `WORKFLOW_EXECUTION_STARTED` followed immediately by `WORKFLOW_EXECUTION_SIGNALED`, and contain two signal events total.

This gives one compact, observable scenario that covers the feature's public contract without duplicating the backend's conflict-policy, request-deduplication, backlink, CHASM/HSM rollout, and cross-namespace validation suites.

## File Map

- Create `features/system_nexus/signal_with_start/feature.go`: caller and target workflows, SDK capability probe, execution setup, result checks, and history checks.
- Create `features/system_nexus/signal_with_start/config.json`: enable `history.enableSignalWithStartFromWorkflow` for a dedicated embedded-server variant.
- Create `features/system_nexus/signal_with_start/README.md`: explain the workflow-to-workflow behavior, same-namespace rule, and assertions.
- Modify `features/features.go`: import and register the Go feature alphabetically.

---

### Task 1: Add and register the Go feature

**Files:**
- Create: `features/system_nexus/signal_with_start/feature.go`
- Modify: `features/features.go`

**Interfaces:**
- Consumes: generated operation reference `workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.SignalWithStartWorkflowExecution`; public `workflow.NewNexusClient`; `harness.Feature`, `harness.Runner`, and `runner.Skip`.
- Produces: `Feature`, `CallerWorkflow`, `TargetWorkflow`, `CallerResult`, and `supportsSystemNexusEndpoint` in package `signal_with_start`.

- [ ] **Step 1: Add the feature implementation and assertions**

Create `features/system_nexus/signal_with_start/feature.go` with this structure:

```go
package signal_with_start

import (
	"context"
	"fmt"
	"time"

	"github.com/temporalio/features/harness/go/harness"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

const (
	systemEndpoint     = "__temporal_system"
	signalName         = "add"
	targetWorkflowName = "SystemNexusSignalWithStartTarget"
)

type CallerResult struct {
	TargetWorkflowID string
	TargetRunID      string
}

func supportsSystemNexusEndpoint() (supported bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if fmt.Sprint(recovered) != "endpoint cannot use reserved __temporal_ prefix" {
				panic(recovered)
			}
			supported = false
		}
	}()
	workflow.NewNexusClient(
		systemEndpoint,
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName,
	)
	return true
}

func CallerWorkflow(
	ctx workflow.Context,
	firstRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
	secondRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (CallerResult, error) {
	nexusClient := workflow.NewNexusClient(
		systemEndpoint,
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName,
	)
	options := workflow.NexusOperationOptions{ScheduleToCloseTimeout: time.Minute}

	var first workflowservice.SignalWithStartWorkflowExecutionResponse
	if err := nexusClient.ExecuteOperation(
		ctx,
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.SignalWithStartWorkflowExecution,
		firstRequest,
		options,
	).Get(ctx, &first); err != nil {
		return CallerResult{}, err
	}
	if !first.GetStarted() {
		return CallerResult{}, fmt.Errorf("first SignalWithStart did not start the target")
	}
	if first.GetRunId() == "" {
		return CallerResult{}, fmt.Errorf("first SignalWithStart returned an empty run ID")
	}

	var second workflowservice.SignalWithStartWorkflowExecutionResponse
	if err := nexusClient.ExecuteOperation(
		ctx,
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.SignalWithStartWorkflowExecution,
		secondRequest,
		options,
	).Get(ctx, &second); err != nil {
		return CallerResult{}, err
	}
	if second.GetStarted() {
		return CallerResult{}, fmt.Errorf("second SignalWithStart unexpectedly started another target")
	}
	if second.GetRunId() != first.GetRunId() {
		return CallerResult{}, fmt.Errorf(
			"SignalWithStart calls targeted different runs: first=%q second=%q",
			first.GetRunId(),
			second.GetRunId(),
		)
	}

	return CallerResult{
		TargetWorkflowID: firstRequest.GetWorkflowId(),
		TargetRunID:      first.GetRunId(),
	}, nil
}

func TargetWorkflow(ctx workflow.Context, initialValue int) (int, error) {
	total := initialValue
	signals := workflow.GetSignalChannel(ctx, signalName)
	for range 2 {
		var value int
		signals.Receive(ctx, &value)
		total += value
	}
	return total, nil
}

func encodePayloads(value any) (*commonpb.Payloads, error) {
	return converter.GetDefaultDataConverter().ToPayloads(value)
}

func newRequest(
	workflowID string,
	taskQueue string,
	workflowInput *commonpb.Payloads,
	signalInput *commonpb.Payloads,
) *workflowservice.SignalWithStartWorkflowExecutionRequest {
	return &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:   workflowID,
		WorkflowType: &commonpb.WorkflowType{Name: targetWorkflowName},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
		Input:        workflowInput,
		SignalName:   signalName,
		SignalInput:  signalInput,
	}
}

var Feature = harness.Feature{
	Workflows: []any{
		CallerWorkflow,
		harness.WorkflowWithOptions{
			Workflow: TargetWorkflow,
			Options:  workflow.RegisterOptions{Name: targetWorkflowName},
		},
	},
	Execute: func(ctx context.Context, runner *harness.Runner) (client.WorkflowRun, error) {
		if !supportsSystemNexusEndpoint() {
			return nil, runner.Skip("Go SDK does not expose the __temporal_system workflow Nexus endpoint")
		}

		workflowInput, err := encodePayloads(1)
		if err != nil {
			return nil, err
		}
		firstSignalInput, err := encodePayloads(20)
		if err != nil {
			return nil, err
		}
		secondSignalInput, err := encodePayloads(22)
		if err != nil {
			return nil, err
		}

		targetWorkflowID := "system-nexus-signal-with-start-" + runner.TaskQueue
		firstRequest := newRequest(targetWorkflowID, runner.TaskQueue, workflowInput, firstSignalInput)
		secondRequest := newRequest(targetWorkflowID, runner.TaskQueue, workflowInput, secondSignalInput)

		return runner.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			TaskQueue:                runner.TaskQueue,
			WorkflowExecutionTimeout: time.Minute,
		}, CallerWorkflow, firstRequest, secondRequest)
	},
	CheckResult: func(ctx context.Context, runner *harness.Runner, run client.WorkflowRun) error {
		var callerResult CallerResult
		if err := run.Get(ctx, &callerResult); err != nil {
			return err
		}
		runner.Require.NotEmpty(callerResult.TargetWorkflowID)
		runner.Require.NotEmpty(callerResult.TargetRunID)

		var targetResult int
		if err := runner.Client.GetWorkflow(
			ctx,
			callerResult.TargetWorkflowID,
			callerResult.TargetRunID,
		).Get(ctx, &targetResult); err != nil {
			return err
		}
		runner.Require.Equal(43, targetResult)
		return nil
	},
	CheckHistory: checkHistory,
}

func readHistory(
	ctx context.Context,
	runner *harness.Runner,
	workflowID string,
	runID string,
) ([]*historypb.HistoryEvent, error) {
	iterator := runner.Client.GetWorkflowHistory(
		ctx,
		workflowID,
		runID,
		false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
	)
	var events []*historypb.HistoryEvent
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func countEvent(events []*historypb.HistoryEvent, eventType enumspb.EventType) int {
	count := 0
	for _, event := range events {
		if event.GetEventType() == eventType {
			count++
		}
	}
	return count
}

func checkHistory(ctx context.Context, runner *harness.Runner, run client.WorkflowRun) error {
	var callerResult CallerResult
	if err := run.Get(ctx, &callerResult); err != nil {
		return err
	}

	callerEvents, err := readHistory(ctx, runner, run.GetID(), run.GetRunID())
	if err != nil {
		return err
	}
	runner.Require.Equal(2, countEvent(callerEvents, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED))
	runner.Require.Equal(2, countEvent(callerEvents, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED))
	runner.Require.Zero(countEvent(callerEvents, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED))

	targetEvents, err := readHistory(
		ctx,
		runner,
		callerResult.TargetWorkflowID,
		callerResult.TargetRunID,
	)
	if err != nil {
		return err
	}
	if len(targetEvents) < 2 {
		return fmt.Errorf("target history has only %d events", len(targetEvents))
	}
	runner.Require.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, targetEvents[0].GetEventType())
	runner.Require.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, targetEvents[1].GetEventType())
	runner.Require.Equal(2, countEvent(targetEvents, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED))
	return nil
}
```

Before accepting the code, confirm the Go compiler version accepts `for range 2`; this repository declares Go 1.26, so the syntax is supported. Keep the request namespace empty in both requests.

- [ ] **Step 2: Register the feature alphabetically**

Add this import near the other `s` imports in `features/features.go`:

```go
system_nexus_signal_with_start "github.com/temporalio/features/features/system_nexus/signal_with_start"
```

Add this registration after `signal_external.Feature` and before `telemetry_metrics.Feature`:

```go
system_nexus_signal_with_start.Feature,
```

- [ ] **Step 3: Compile the feature on the pinned SDK**

Run:

```bash
go -C features test . ./system_nexus/signal_with_start
```

Expected: PASS. Compilation must succeed on v1.49.0 even though execution will skip because the generated operation reference and public generic Nexus API already exist.

- [ ] **Step 4: Run the feature on the pinned SDK and verify the compatibility skip**

Run:

```bash
go run . run --lang go --no-history-check system_nexus/signal_with_start
```

Expected: the feature is reported as skipped with `Go SDK does not expose the __temporal_system workflow Nexus endpoint`; there must be no worker panic.

- [ ] **Step 5: Commit only if explicitly authorized**

If the user explicitly requests a commit, run:

```bash
git add features/system_nexus/signal_with_start/feature.go features/features.go
git commit -m "test: add system nexus signal-with-start feature"
```

Otherwise leave the changes uncommitted.

---

### Task 2: Enable the server feature for the test

**Files:**
- Create: `features/system_nexus/signal_with_start/config.json`

**Interfaces:**
- Consumes: `RunFeatureConfig.RunVariants` and `Runner.dynamicConfigArgs`.
- Produces: one embedded-server variant named `feature-enabled` with `history.enableSignalWithStartFromWorkflow=true`.

- [ ] **Step 1: Run against a System-Nexus-capable SDK before adding config**

After a public Go SDK checkout permits `__temporal_system`, run the feature with that checkout through the repository's supported local-SDK mechanism:

```bash
go run . run --lang go --version ../sdk-go --no-history-check system_nexus/signal_with_start
```

Expected: FAIL with a Nexus operation error containing `SignalWithStart operation is disabled`. This proves the server gate is active and the test reaches the reserved operation.

- [ ] **Step 2: Add the dynamic-config variant**

Create `features/system_nexus/signal_with_start/config.json`:

```json
{
  "runVariants": [
    {
      "name": "feature-enabled",
      "dynamicConfig": {
        "history.enableSignalWithStartFromWorkflow": true
      }
    }
  ]
}
```

Do not add `nexusoperation.enableChasmWorkflowOperations`. The feature test owns the public behavior, while backend integration tests own the CHASM/HSM rollout matrix.

- [ ] **Step 3: Run the enabled feature with the capable SDK checkout**

Run:

```bash
go run . run --lang go --version ../sdk-go --no-history-check system_nexus/signal_with_start
```

Expected: PASS as `system_nexus/signal_with_start#feature-enabled`. The caller result must identify one target run, the target result must be `43`, the caller history must contain two synchronous Nexus completions, and the target history must contain two signals.

- [ ] **Step 4: Verify that no user Nexus endpoint is created**

Run:

```bash
go run . run --lang go --version ../sdk-go --no-history-check system_nexus/signal_with_start 2>&1 | tee /tmp/system-nexus-signal-with-start.log
rg "features-nexus-|CreateNexusEndpoint|NexusEndpoint" /tmp/system-nexus-signal-with-start.log
```

Expected: the feature run passes, and the `rg` command returns no matches. The reserved system endpoint is server-owned and must not use `OperatorService.CreateNexusEndpoint`.

- [ ] **Step 5: Commit only if explicitly authorized**

If the user explicitly requests a commit, run:

```bash
git add features/system_nexus/signal_with_start/config.json
git commit -m "test: enable signal-with-start system nexus variant"
```

Otherwise leave the changes uncommitted.

---

### Task 3: Document the feature and its boundaries

**Files:**
- Create: `features/system_nexus/signal_with_start/README.md`

**Interfaces:**
- Consumes: the behavior and assertions implemented by Tasks 1 and 2.
- Produces: the feature's durable specification for future SDK-language implementations.

- [ ] **Step 1: Add the feature README**

Create `features/system_nexus/signal_with_start/README.md`:

```markdown
# Signal with start from a workflow

A workflow invokes the reserved System Nexus
`SignalWithStartWorkflowExecution` operation to communicate with another
workflow in the same namespace. The first call starts the target and delivers a
signal; the second call signals the already-running target.

The operation uses endpoint `__temporal_system`, service
`temporal.api.workflowservice.v1.WorkflowService`, and operation
`SignalWithStartWorkflowExecution`. The request omits its namespace so the
server derives it from the caller workflow context. System Nexus does not
support targeting a different namespace.

# Detailed spec

- The first operation response has `started = true` and a non-empty run ID.
- The second operation response has `started = false` and the same run ID.
- The target receives its start input and both signal payloads, returning `43`.
- The target history starts with `WorkflowExecutionStarted` followed by
  `WorkflowExecutionSignaled`.
- The caller records two synchronous Nexus operations: each transitions from
  scheduled directly to completed without a started event.

The feature uses an embedded dev-server run variant to enable
`history.enableSignalWithStartFromWorkflow`. SDKs that do not expose the
reserved System Nexus endpoint skip the feature.
```

- [ ] **Step 2: Check documentation against executable assertions**

Run:

```bash
rg -n "Started|RunId|NEXUS_OPERATION_|WORKFLOW_EXECUTION_SIGNALED|43" \
  features/system_nexus/signal_with_start/feature.go \
  features/system_nexus/signal_with_start/README.md
```

Expected: every README claim maps to a concrete workflow validation, result assertion, or history assertion in `feature.go`.

- [ ] **Step 3: Commit only if explicitly authorized**

If the user explicitly requests a commit, run:

```bash
git add features/system_nexus/signal_with_start/README.md
git commit -m "docs: describe system nexus signal-with-start feature"
```

Otherwise leave the changes uncommitted.

---

### Task 4: Verify repository integration

**Files:**
- Verify: `features/system_nexus/signal_with_start/feature.go`
- Verify: `features/system_nexus/signal_with_start/config.json`
- Verify: `features/system_nexus/signal_with_start/README.md`
- Verify: `features/features.go`

**Interfaces:**
- Consumes: the complete feature and the repository's Go runner.
- Produces: evidence that the feature compiles on the pinned SDK, skips safely before SDK support, and passes against a capable SDK checkout.

- [ ] **Step 1: Format the Go files**

Run:

```bash
gofmt -w features/system_nexus/signal_with_start/feature.go features/features.go
```

Expected: command exits successfully and changes only formatting.

- [ ] **Step 2: Run focused unit and compile checks**

Run:

```bash
go test ./cmd ./sdkbuild
go -C harness/go test ./cmd ./harness
go -C features test . ./system_nexus/signal_with_start
```

Expected: PASS.

- [ ] **Step 3: Verify the current pinned-SDK behavior**

Run:

```bash
go run . run --lang go --no-history-check system_nexus/signal_with_start
```

Expected while v1.49.0 is pinned: SKIP with the explicit unsupported-system-endpoint reason.

- [ ] **Step 4: Verify the end-to-end behavior with the capable SDK checkout**

Run:

```bash
go run . run --lang go --version ../sdk-go --no-history-check system_nexus/signal_with_start
```

Expected: PASS for `system_nexus/signal_with_start#feature-enabled`.

- [ ] **Step 5: Run the broader Go test suite**

Run:

```bash
go test ./...
```

Expected: PASS.

- [ ] **Step 6: Check the final diff and worktree**

Run:

```bash
git diff --check
git status --short
git diff -- features/features.go features/system_nexus/signal_with_start
```

Expected: no whitespace errors; only the four planned paths are changed or created, aside from unrelated pre-existing user changes.

- [ ] **Step 7: Commit only if explicitly authorized**

If the user explicitly requests one squashed verification commit instead of the per-task commits, run:

```bash
git add features/features.go features/system_nexus/signal_with_start
git commit -m "test: cover workflow signal-with-start via system nexus"
```

Otherwise leave all changes uncommitted.

## Explicit Non-Goals

- Foreign-namespace rejection: the server processor already has direct validation coverage; this feature harness provisions one namespace and should focus on the supported same-namespace contract.
- Conflict and reuse policy matrices, start delay, terminated targets, request deduplication, and backlinks: covered by backend integration tests.
- CHASM versus legacy HSM rollout variants: an internal server implementation dimension, not an SDK feature contract.
- Codec, external storage, and serialization-context matrix: important System Nexus coverage, but large enough for a separate feature directory and plan.
- Java, TypeScript, Python, .NET, PHP, and Ruby implementations: add language files to this same feature directory only after each SDK exposes its public System Nexus API.

## Failure Modes and Diagnostics

- Panic containing `endpoint cannot use reserved __temporal_ prefix`: SDK lacks public System Nexus workflow support; the capability probe must convert this into a skip.
- Nexus failure containing `SignalWithStart operation is disabled`: `config.json` was not applied or the run used an external server without the flag enabled.
- `CreateNexusEndpoint` permission or provisioning errors: the feature was placed under the wrong directory prefix.
- First response reports `Started=false`: target workflow ID collided with another execution; ensure it includes the unique feature task queue.
- Second response reports `Started=true`: target completed before the second operation; keep `TargetWorkflow` waiting for exactly two signals.
- Different response run IDs: the second call did not reuse the running target, which violates the tested contract.
- Target result hangs: target worker was not registered on the feature task queue, signal names differ, or one signal payload was not delivered.
- Target history does not begin with started then signaled: the server did not preserve Signal-With-Start event ordering.

## Self-Review Result

- Spec coverage: same-namespace derivation, missing-target start, existing-target signal, synchronous Nexus history, server feature gating, and SDK compatibility are each mapped to an executable task.
- Scope: one feature directory and one registry edit; no runner or backend changes are required.
- Type consistency: both operation calls use `SignalWithStartWorkflowExecutionRequest` and decode `SignalWithStartWorkflowExecutionResponse`; `CallerResult` is used consistently by result and history checks.
- Safety: no commit or push occurs without explicit user authorization; no internal SDK APIs or user Nexus endpoints are used.
