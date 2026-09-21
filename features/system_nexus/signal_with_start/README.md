# Signal with start from a workflow

A workflow uses the reserved System Nexus
`SignalWithStartWorkflowExecution` operation to communicate with another
workflow in the same namespace. The first call starts the target and delivers
a signal; the second call signals the same running target.

The operation uses endpoint `__temporal_system`, service
`temporal.api.workflowservice.v1.WorkflowService`, and operation
`SignalWithStartWorkflowExecution`. The low-level Go request omits its namespace
so the server derives it from the caller workflow. System Nexus does not support
targeting a different namespace.

## Shared contract

- Both calls use workflow ID conflict policy `USE_EXISTING`.
- The first call supplies the target's start input and first signal.
- The second call supplies a deliberately unused start input and the second
  signal.
- The second call reuses the existing target execution, so both calls address
  the same run.
- Result shapes are language-specific: Go asserts numeric result `43` from the
  start input and both signal values; Python and .NET assert ordered
  `started:` and `signal:` strings.
- The feature runs with `history.enableSignalWithStartFromWorkflow=true` on an
  embedded dev server.

## Language coverage

| Language | API surface | Additional assertions |
|---|---|---|
| Go | Generated WorkflowService Nexus operation | First response started the target, both responses name the same run, caller Nexus history is synchronous, and target history starts with start then signal |
| Python | `workflow.signal_with_start_workflow` | Public workflow API delivers the original start value and both signals in order |
| .NET | `Workflow.SignalWithStartWorkflowAsync` | Public workflow API delivers the original start value and both signals in order |

The pinned Go SDK v1.49.0 still rejects the reserved endpoint, so the Go feature
reports a harness skip until a release exposes workflow-side System Nexus
Signal-With-Start. Python and .NET provide executable end-to-end coverage in the
meantime.

## Scope boundary

This feature does not test custom payload converters, codecs, or external
storage. Those components traverse the inner payloads of a System Nexus envelope
and have SDK-specific compatibility requirements, so those checks are reserved
for a separate follow-up feature/plan and are not covered here.
