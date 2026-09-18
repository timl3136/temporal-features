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
`history.enableSignalWithStartFromWorkflow`. With the pinned Go SDK v1.49.0,
the public workflow Nexus client rejects the reserved `__temporal_` endpoint
prefix; the feature converts that known capability failure into a skip. The
feature is a dormant scaffold until a public SDK release exposes the reserved
System Nexus endpoint.
